//! Buffered pipeline step metrics (same semantics as `sentry_streams.metrics.stats`).
//! Records via the `metrics` crate only — no Arroyo types.
//!
//! State is **thread-local**: each OS thread has its own buffers. The consumer
//! `submit` path is expected to call into this from one thread per pipeline; do
//! not rely on cross-thread aggregation here.

use std::cell::RefCell;
use std::collections::HashMap;
use std::time::{Duration, Instant};

const METRIC_INPUT_MESSAGES: &str = "streams.pipeline.input.messages";
const METRIC_ERRORS: &str = "streams.pipeline.errors";
const METRIC_DURATION: &str = "streams.pipeline.duration";

thread_local! {
    static PIPELINE_STATS: RefCell<PipelineStatsState> = RefCell::new(PipelineStatsState::new());
}

/// Per-thread pipeline stats buffers and flush timer.
struct PipelineStatsState {
    exec_buffer: HashMap<String, u64>,
    error_buffer: HashMap<String, u64>,
    timing_buffer: HashMap<String, f64>,
    last_flush: Instant,
    flush_interval: Duration,
}

impl PipelineStatsState {
    fn new() -> Self {
        Self::with_flush_interval(Duration::from_secs(10))
    }

    fn with_flush_interval(flush_interval: Duration) -> Self {
        Self {
            exec_buffer: HashMap::new(),
            error_buffer: HashMap::new(),
            timing_buffer: HashMap::new(),
            last_flush: Instant::now(),
            flush_interval,
        }
    }

    fn flush_emit(&mut self) {
        for (step, value) in self.exec_buffer.drain() {
            let labels = vec![("step".to_string(), step)];
            metrics::counter!(METRIC_INPUT_MESSAGES, &labels).increment(value);
        }
        for (step, value) in self.error_buffer.drain() {
            let labels = vec![("step".to_string(), step)];
            metrics::counter!(METRIC_ERRORS, &labels).increment(value);
        }
        for (step, fvalue) in self.timing_buffer.drain() {
            let labels = vec![("step".to_string(), step)];
            metrics::histogram!(METRIC_DURATION, &labels).record(fvalue);
        }
        self.last_flush = Instant::now();
    }

    fn maybe_flush(&mut self) {
        if self.last_flush.elapsed() >= self.flush_interval {
            self.flush_emit();
        }
    }

    fn step_exec(&mut self, step: &str) {
        *self.exec_buffer.entry(step.to_string()).or_insert(0) += 1;
        self.maybe_flush();
    }

    fn step_error(&mut self, step: &str) {
        *self.error_buffer.entry(step.to_string()).or_insert(0) += 1;
        self.maybe_flush();
    }

    fn step_timing(&mut self, step: &str, value_secs: f64) {
        let entry = self.timing_buffer.entry(step.to_string()).or_insert(0.0);
        if *entry < value_secs {
            *entry = value_secs;
        }
        self.maybe_flush();
    }

    #[cfg(test)]
    fn exec_count(&self, step: &str) -> u64 {
        self.exec_buffer.get(step).copied().unwrap_or(0)
    }

    #[cfg(test)]
    fn flush_emit_for_test(&mut self) {
        self.flush_emit();
    }
}

/// Handle to this thread's [`PipelineStatsState`]; cheap to copy (zero-sized).
#[derive(Clone, Copy)]
pub struct PipelineStats;

impl PipelineStats {
    pub fn step_exec(self, step: &str) {
        PIPELINE_STATS.with(|cell| {
            let mut s = cell.borrow_mut();
            s.step_exec(step);
        });
    }

    pub fn step_error(self, step: &str) {
        PIPELINE_STATS.with(|cell| {
            let mut s = cell.borrow_mut();
            s.step_error(step);
        });
    }

    pub fn step_timing(self, step: &str, value_secs: f64) {
        PIPELINE_STATS.with(|cell| {
            let mut s = cell.borrow_mut();
            s.step_timing(step, value_secs);
        });
    }

    #[cfg(test)]
    pub(crate) fn flush_emit_for_test(self) {
        PIPELINE_STATS.with(|cell| {
            let mut s = cell.borrow_mut();
            s.flush_emit_for_test();
        });
    }

    #[cfg(test)]
    pub(crate) fn exec_count(self, step: &str) -> u64 {
        PIPELINE_STATS.with(|cell| {
            let s = cell.borrow();
            s.exec_count(step)
        })
    }
}

pub fn get_stats() -> PipelineStats {
    PipelineStats
}

#[cfg(test)]
pub(crate) fn configure_pipeline_stats_for_test(flush_interval: Duration) {
    PIPELINE_STATS.with(|cell| {
        *cell.borrow_mut() = PipelineStatsState::with_flush_interval(flush_interval);
    });
}

#[cfg(test)]
pub(crate) fn reset_pipeline_stats_for_test() {
    configure_pipeline_stats_for_test(Duration::from_secs(10));
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::{Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct CaptureRecorder {
        counters: Arc<Mutex<Vec<(Key, u64)>>>,
        histograms: Arc<Mutex<Vec<(Key, f64)>>>,
    }

    impl Recorder for CaptureRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

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
            self.counters
                .lock()
                .unwrap()
                .push((self.key.clone(), value));
        }

        fn absolute(&self, _value: u64) {}
    }

    struct CaptureHistogram {
        key: Key,
        histograms: Arc<Mutex<Vec<(Key, f64)>>>,
    }

    impl metrics::HistogramFn for CaptureHistogram {
        fn record(&self, value: f64) {
            self.histograms
                .lock()
                .unwrap()
                .push((self.key.clone(), value));
        }
    }

    fn key_name(key: &Key) -> String {
        key.name().to_string()
    }

    fn labels_include_step(key: &Key, step: &str) -> bool {
        key.labels().any(|l| l.key() == "step" && l.value() == step)
    }

    fn prepare_pipeline_stats_for_test(flush_interval: Duration) {
        reset_pipeline_stats_for_test();
        configure_pipeline_stats_for_test(flush_interval);
    }

    /// Reset pipeline stats, apply flush interval, and wire a [`CaptureRecorder`] for `metrics` locals.
    struct CaptureTestContext {
        counters: Arc<Mutex<Vec<(Key, u64)>>>,
        histograms: Arc<Mutex<Vec<(Key, f64)>>>,
        recorder: Arc<CaptureRecorder>,
    }

    impl CaptureTestContext {
        fn new(flush_interval: Duration) -> Self {
            prepare_pipeline_stats_for_test(flush_interval);
            let counters = Arc::new(Mutex::new(Vec::new()));
            let histograms = Arc::new(Mutex::new(Vec::new()));
            let recorder = Arc::new(CaptureRecorder {
                counters: Arc::clone(&counters),
                histograms: Arc::clone(&histograms),
            });
            Self {
                counters,
                histograms,
                recorder,
            }
        }

        fn install_metrics_recorder(&self) -> metrics::LocalRecorderGuard<'_> {
            metrics::set_default_local_recorder(self.recorder.as_ref())
        }
    }

    #[test]
    fn flush_emits_aggregated_counters_and_max_timing() {
        let ctx = CaptureTestContext::new(Duration::from_secs(3600));
        let stats = get_stats();
        stats.step_exec("in_step");
        stats.step_exec("in_step");
        stats.step_error("err_step");
        stats.step_timing("timer_step", 0.1);
        stats.step_timing("timer_step", 0.05);
        let _guard = ctx.install_metrics_recorder();
        stats.flush_emit_for_test();

        let c = ctx.counters.lock().unwrap();
        let input: u64 = c
            .iter()
            .filter(|(k, _)| {
                key_name(k) == METRIC_INPUT_MESSAGES && labels_include_step(k, "in_step")
            })
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(input, 2);

        let errs: u64 = c
            .iter()
            .filter(|(k, _)| key_name(k) == METRIC_ERRORS && labels_include_step(k, "err_step"))
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(errs, 1);

        let h = ctx.histograms.lock().unwrap();
        let dur: Vec<f64> = h
            .iter()
            .filter(|(k, _)| key_name(k) == METRIC_DURATION && labels_include_step(k, "timer_step"))
            .map(|(_, v)| *v)
            .collect();
        assert_eq!(dur, vec![0.1]);
    }

    #[test]
    fn no_auto_flush_before_interval() {
        prepare_pipeline_stats_for_test(Duration::from_secs(3600));
        let stats = get_stats();
        stats.step_exec("a");
        assert_eq!(stats.exec_count("a"), 1);
    }

    #[test]
    fn flush_clears_buffers() {
        let ctx = CaptureTestContext::new(Duration::from_secs(3600));
        let stats = get_stats();
        stats.step_exec("s");
        let _guard = ctx.install_metrics_recorder();
        stats.flush_emit_for_test();
        assert_eq!(
            ctx.counters
                .lock()
                .unwrap()
                .iter()
                .filter(|(k, _)| labels_include_step(k, "s"))
                .count(),
            1
        );
        ctx.counters.lock().unwrap().clear();
        stats.step_exec("s");
        stats.flush_emit_for_test();
        let c = ctx.counters.lock().unwrap();
        let sum: u64 = c
            .iter()
            .filter(|(k, _)| key_name(k) == METRIC_INPUT_MESSAGES && labels_include_step(k, "s"))
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(sum, 1);
    }

    #[test]
    fn multiple_steps_one_flush() {
        let ctx = CaptureTestContext::new(Duration::from_secs(3600));
        let stats = get_stats();
        stats.step_exec("step_1");
        stats.step_exec("step_1");
        stats.step_exec("step_2");
        let _guard = ctx.install_metrics_recorder();
        stats.flush_emit_for_test();

        let c = ctx.counters.lock().unwrap();
        let n1: u64 = c
            .iter()
            .filter(|(k, _)| {
                key_name(k) == METRIC_INPUT_MESSAGES && labels_include_step(k, "step_1")
            })
            .map(|(_, v)| *v)
            .sum();
        let n2: u64 = c
            .iter()
            .filter(|(k, _)| {
                key_name(k) == METRIC_INPUT_MESSAGES && labels_include_step(k, "step_2")
            })
            .map(|(_, v)| *v)
            .sum();
        assert_eq!(n1, 2);
        assert_eq!(n2, 1);
    }

    #[test]
    fn throttle_flushes_after_interval() {
        let ctx = CaptureTestContext::new(Duration::from_millis(50));
        let stats = get_stats();
        let _guard = ctx.install_metrics_recorder();
        stats.step_exec("throttle_step");
        assert!(ctx.counters.lock().unwrap().is_empty());

        std::thread::sleep(Duration::from_millis(120));
        stats.step_exec("throttle_step");

        let c = ctx.counters.lock().unwrap();
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
