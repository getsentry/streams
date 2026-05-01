//! Buffered pipeline step metrics (same semantics as `sentry_streams.metrics.stats`).
//! Records via the `metrics` crate only — no Arroyo types.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

const METRIC_INPUT_MESSAGES: &str = "streams.pipeline.input.messages";
const METRIC_ERRORS: &str = "streams.pipeline.errors";
const METRIC_DURATION: &str = "streams.pipeline.duration";

struct Inner {
    exec_buffer: HashMap<String, u64>,
    error_buffer: HashMap<String, u64>,
    timing_buffer: HashMap<String, f64>,
    last_flush: Instant,
}

impl Inner {
    fn new() -> Self {
        Self {
            exec_buffer: HashMap::new(),
            error_buffer: HashMap::new(),
            timing_buffer: HashMap::new(),
            last_flush: Instant::now(),
        }
    }
}

/// Aggregates per-step exec / error / timing and flushes periodically to DogStatsD via `metrics`.
pub struct PipelineStats {
    inner: Mutex<Inner>,
    flush_interval: Duration,
}

impl PipelineStats {
    fn with_flush_interval(flush_interval: Duration) -> Self {
        Self {
            inner: Mutex::new(Inner::new()),
            flush_interval,
        }
    }

    /// Production flush interval (10 seconds), matching Python `FLUSH_TIME`.
    pub fn new() -> Self {
        Self::with_flush_interval(Duration::from_secs(10))
    }

    fn flush_emit_locked(inner: &mut Inner) {
        for (step, value) in inner.exec_buffer.drain() {
            let labels = vec![("step".to_string(), step)];
            metrics::counter!(METRIC_INPUT_MESSAGES, &labels).increment(value);
        }
        for (step, value) in inner.error_buffer.drain() {
            let labels = vec![("step".to_string(), step)];
            metrics::counter!(METRIC_ERRORS, &labels).increment(value);
        }
        for (step, fvalue) in inner.timing_buffer.drain() {
            let labels = vec![("step".to_string(), step)];
            metrics::histogram!(METRIC_DURATION, &labels).record(fvalue);
        }
        inner.last_flush = Instant::now();
    }

    fn maybe_flush(&self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.last_flush.elapsed() >= self.flush_interval {
            Self::flush_emit_locked(&mut inner);
        }
    }

    pub fn step_exec(&self, step: &str) {
        let mut inner = self.inner.lock().unwrap();
        *inner.exec_buffer.entry(step.to_string()).or_insert(0) += 1;
        drop(inner);
        self.maybe_flush();
    }

    pub fn step_error(&self, step: &str) {
        let mut inner = self.inner.lock().unwrap();
        *inner.error_buffer.entry(step.to_string()).or_insert(0) += 1;
        drop(inner);
        self.maybe_flush();
    }

    pub fn step_timing(&self, step: &str, value_secs: f64) {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.timing_buffer.entry(step.to_string()).or_insert(0.0);
        if *entry < value_secs {
            *entry = value_secs;
        }
        drop(inner);
        self.maybe_flush();
    }

    #[cfg(test)]
    fn with_flush_interval_for_test(flush_interval: Duration) -> Self {
        Self::with_flush_interval(flush_interval)
    }

    /// Emit and clear buffers ignoring the throttle (for unit tests).
    #[cfg(test)]
    pub(crate) fn flush_emit_for_test(&self) {
        let mut inner = self.inner.lock().unwrap();
        Self::flush_emit_locked(&mut inner);
    }

    #[cfg(test)]
    pub(crate) fn exec_count(&self, step: &str) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .exec_buffer
            .get(step)
            .copied()
            .unwrap_or(0)
    }

}

static GLOBAL_STATS: OnceLock<PipelineStats> = OnceLock::new();

pub fn get_stats() -> &'static PipelineStats {
    GLOBAL_STATS.get_or_init(PipelineStats::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::{Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use std::sync::Arc;

    #[derive(Default)]
    struct CaptureRecorder {
        counters: Arc<Mutex<Vec<(Key, u64)>>>,
        histograms: Arc<Mutex<Vec<(Key, f64)>>>,
    }

    impl Recorder for CaptureRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Counter {
            metrics::Counter::from_arc(Arc::new(CaptureCounter {
                key: key.clone(),
                counters: Arc::clone(&self.counters),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> metrics::Gauge {
            metrics::Gauge::noop()
        }

        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Histogram {
            metrics::Histogram::from_arc(Arc::new(CaptureHistogram {
                key: key.clone(),
                histograms: Arc::clone(&self.histograms),
            }))
        }
    }

    struct CaptureCounter {
        key: Key,
        counters: Arc<Mutex<Vec<(Key, u64)>>>,
    }

    impl metrics::CounterFn for CaptureCounter {
        fn increment(&self, value: u64) {
            self.counters.lock().unwrap().push((self.key.clone(), value));
        }

        fn absolute(&self, _value: u64) {}
    }

    struct CaptureHistogram {
        key: Key,
        histograms: Arc<Mutex<Vec<(Key, f64)>>>,
    }

    impl metrics::HistogramFn for CaptureHistogram {
        fn record(&self, value: f64) {
            self.histograms.lock().unwrap().push((self.key.clone(), value));
        }
    }

    fn key_name(key: &Key) -> String {
        key.name().to_string()
    }

    fn labels_include_step(key: &Key, step: &str) -> bool {
        key.labels().any(|l| l.key() == "step" && l.value() == step)
    }

    #[test]
    fn flush_emits_aggregated_counters_and_max_timing() {
        let counters = Arc::new(Mutex::new(Vec::new()));
        let histograms = Arc::new(Mutex::new(Vec::new()));
        let rec = Arc::new(CaptureRecorder {
            counters: Arc::clone(&counters),
            histograms: Arc::clone(&histograms),
        });
        let stats = PipelineStats::with_flush_interval_for_test(Duration::from_secs(3600));
        stats.step_exec("in_step");
        stats.step_exec("in_step");
        stats.step_error("err_step");
        stats.step_timing("timer_step", 0.1);
        stats.step_timing("timer_step", 0.05);
        let _guard = metrics::set_default_local_recorder(rec.as_ref());
        stats.flush_emit_for_test();

        let c = counters.lock().unwrap();
        let input: u64 = c
            .iter()
            .filter(|(k, _)| key_name(k) == METRIC_INPUT_MESSAGES && labels_include_step(k, "in_step"))
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(input, 2);

        let errs: u64 = c
            .iter()
            .filter(|(k, _)| key_name(k) == METRIC_ERRORS && labels_include_step(k, "err_step"))
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(errs, 1);

        let h = histograms.lock().unwrap();
        let dur: Vec<f64> = h
            .iter()
            .filter(|(k, _)| key_name(k) == METRIC_DURATION && labels_include_step(k, "timer_step"))
            .map(|(_, v)| *v)
            .collect();
        assert_eq!(dur, vec![0.1]);
    }

    #[test]
    fn no_auto_flush_before_interval() {
        let stats = PipelineStats::with_flush_interval_for_test(Duration::from_secs(3600));
        stats.step_exec("a");
        assert_eq!(stats.exec_count("a"), 1);
    }

    #[test]
    fn flush_clears_buffers() {
        let counters = Arc::new(Mutex::new(Vec::new()));
        let histograms = Arc::new(Mutex::new(Vec::new()));
        let rec = Arc::new(CaptureRecorder {
            counters: Arc::clone(&counters),
            histograms: Arc::clone(&histograms),
        });
        let stats = PipelineStats::with_flush_interval_for_test(Duration::from_secs(3600));
        stats.step_exec("s");
        let _guard = metrics::set_default_local_recorder(rec.as_ref());
        stats.flush_emit_for_test();
        assert_eq!(
            counters
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| labels_include_step(k, "s"))
                .count(),
            1
        );
        counters.lock().unwrap().clear();
        stats.step_exec("s");
        stats.flush_emit_for_test();
        let c = counters.lock().unwrap();
        let sum: u64 = c
            .iter()
            .filter(|(k, _)| key_name(k) == METRIC_INPUT_MESSAGES && labels_include_step(k, "s"))
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(sum, 1);
    }

    #[test]
    fn multiple_steps_one_flush() {
        let counters = Arc::new(Mutex::new(Vec::new()));
        let histograms = Arc::new(Mutex::new(Vec::new()));
        let rec = Arc::new(CaptureRecorder {
            counters: Arc::clone(&counters),
            histograms: Arc::clone(&histograms),
        });
        let stats = PipelineStats::with_flush_interval_for_test(Duration::from_secs(3600));
        stats.step_exec("step_1");
        stats.step_exec("step_1");
        stats.step_exec("step_2");
        let _guard = metrics::set_default_local_recorder(rec.as_ref());
        stats.flush_emit_for_test();

        let c = counters.lock().unwrap();
        let n1: u64 = c
            .iter()
            .filter(|(k, _)| key_name(k) == METRIC_INPUT_MESSAGES && labels_include_step(k, "step_1"))
            .map(|(_, v)| *v)
            .sum();
        let n2: u64 = c
            .iter()
            .filter(|(k, _)| key_name(k) == METRIC_INPUT_MESSAGES && labels_include_step(k, "step_2"))
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(n1, 2);
        assert_eq!(n2, 1);
    }

    #[test]
    fn throttle_flushes_after_interval() {
        let counters = Arc::new(Mutex::new(Vec::new()));
        let histograms = Arc::new(Mutex::new(Vec::new()));
        let rec = Arc::new(CaptureRecorder {
            counters: Arc::clone(&counters),
            histograms: Arc::clone(&histograms),
        });
        let stats = PipelineStats::with_flush_interval_for_test(Duration::from_millis(50));
        let _guard = metrics::set_default_local_recorder(rec.as_ref());
        stats.step_exec("throttle_step");
        assert!(counters.lock().unwrap().is_empty());

        std::thread::sleep(Duration::from_millis(120));
        stats.step_exec("throttle_step");

        let c = counters.lock().unwrap();
        let sum: u64 = c
            .iter()
            .filter(|(k, _)| {
                key_name(k) == METRIC_INPUT_MESSAGES && labels_include_step(k, "throttle_step")
            })
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(sum, 2, "expected throttle flush to emit both execs");
    }
}
