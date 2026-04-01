use crate::metrics_config::PyMetricConfig;
use metrics::Label;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use sentry_arroyo::metrics::{init as arroyo_init, Metric, MetricType, Recorder};
use std::sync::OnceLock;
use std::time::Duration;
use tracing::{error, info, warn};

/// Prefix for all stream pipeline metrics emitted from this crate (matches Arroyo facade keys).
pub const PIPELINE_METRIC_PREFIX: &str = "streams.pipeline";

static STREAMS_RECORDER: OnceLock<StreamsMetricsRecorder> = OnceLock::new();

/// Installs [`StreamsMetricsRecorder`] so [`record_counter`] / [`record_histogram`] emit. For unit tests; first call wins.
#[cfg(test)]
pub(crate) fn init_streams_recorder_for_tests() {
    let _ = STREAMS_RECORDER.set(StreamsMetricsRecorder);
}

/// Records pipeline metrics with the standard prefix. Configured default tags come from DogStatsD
/// [`DogStatsDBuilder::with_global_labels`] (not duplicated here).
#[derive(Debug, Default, Clone, Copy)]
pub struct StreamsMetricsRecorder;

impl StreamsMetricsRecorder {
    fn labels(extra: &[(&str, &str)]) -> Vec<(String, String)> {
        extra
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn full_key(name: &str) -> String {
        format!("{}.{}", PIPELINE_METRIC_PREFIX, name)
    }

    pub fn record_counter(&self, name: &str, extra: &[(&str, &str)], value: u64) {
        let key = Self::full_key(name);
        let labels = Self::labels(extra);
        metrics::counter!(key, &labels).increment(value);
    }

    pub fn record_histogram(&self, name: &str, extra: &[(&str, &str)], value: f64) {
        let key = Self::full_key(name);
        let labels = Self::labels(extra);
        metrics::histogram!(key, &labels).record(value);
    }

    pub fn record_gauge(&self, name: &str, extra: &[(&str, &str)], value: f64) {
        let key = Self::full_key(name);
        let labels = Self::labels(extra);
        metrics::gauge!(key, &labels).set(value);
    }
}

/// Records a counter if [`configure_metrics`] has installed the recorder; otherwise a no-op (same idea as Arroyo's global recorder).
pub(crate) fn record_counter(name: &str, extra: &[(&str, &str)], value: u64) {
    if let Some(r) = STREAMS_RECORDER.get() {
        r.record_counter(name, extra, value);
    }
}

/// Records a histogram sample if the recorder is installed; otherwise a no-op.
pub(crate) fn record_histogram(name: &str, extra: &[(&str, &str)], value: f64) {
    if let Some(r) = STREAMS_RECORDER.get() {
        r.record_histogram(name, extra, value);
    }
}

/// Records a gauge if the recorder is installed; otherwise a no-op.
#[allow(dead_code)]
pub(crate) fn record_gauge(name: &str, extra: &[(&str, &str)], value: f64) {
    if let Some(r) = STREAMS_RECORDER.get() {
        r.record_gauge(name, extra, value);
    }
}

struct MetricsFacadeRecorder;

impl Recorder for MetricsFacadeRecorder {
    fn record_metric(&self, metric: Metric<'_>) {
        let key = format!("{}.{}", PIPELINE_METRIC_PREFIX, metric.key);
        let value_f64 = match metric.value {
            sentry_arroyo::metrics::MetricValue::I64(v) => v as f64,
            sentry_arroyo::metrics::MetricValue::U64(v) => v as f64,
            sentry_arroyo::metrics::MetricValue::F64(v) => v,
            sentry_arroyo::metrics::MetricValue::Duration(d) => d.as_millis() as f64,
            _ => return,
        };

        let labels: Vec<(String, String)> = metric
            .tags
            .iter()
            .map(|(key, value_dyn)| (key.to_string(), format!("{}", value_dyn)))
            .collect();

        match metric.ty {
            MetricType::Counter => {
                let counter = metrics::counter!(key, &labels);
                counter.increment(value_f64 as u64);
            }
            MetricType::Gauge => {
                let gauge = metrics::gauge!(key, &labels);
                gauge.set(value_f64);
            }
            MetricType::Timer => {
                let histogram = metrics::histogram!(key, &labels);
                histogram.record(value_f64);
            }
            _ => {}
        }
    }
}

pub fn configure_metrics(metric_config: Option<PyMetricConfig>) {
    if let Some(ref metric_config) = metric_config {
        let host = metric_config.host();
        let port = metric_config.port();

        info!("Initializing metrics with host: {}:{}", host, port);

        let mut builder = DogStatsDBuilder::default()
            .with_remote_address(&format!("{}:{}", host, port))
            .expect("Failed to parse address");

        let default_tags = metric_config.tags().unwrap_or_default();
        if !default_tags.is_empty() {
            info!(
                "Configuring metrics with {} global labels",
                default_tags.len()
            );
            let labels: Vec<Label> = default_tags
                .iter()
                .map(|(key, value)| Label::new(key.clone(), value.clone()))
                .collect();
            for label in &labels {
                info!("  - {}: {}", label.key(), label.value());
            }
            builder = builder.with_global_labels(labels);
        }

        if let Some(flush_interval_ms) = metric_config.flush_interval_ms() {
            let duration = Duration::from_millis(flush_interval_ms);
            info!("Configuring metrics flush interval to {:?}", duration);
            builder = builder.with_flush_interval(duration);
        }

        match builder.install() {
            Ok(_) => {
                info!("Successfully initialized DogStatsD metrics exporter");
            }
            Err(e) => {
                error!("Failed to install DogStatsD recorder: {}", e);
                return;
            }
        }

        if STREAMS_RECORDER.set(StreamsMetricsRecorder).is_err() {
            warn!("Streams pipeline metrics recorder was already initialized; skipping");
        }

        if arroyo_init(MetricsFacadeRecorder).is_err() {
            warn!("Arroyo metrics recorder already initialized, skipping");
            return;
        }

        info!("Successfully initialized arroyo metrics");
    }
}

/// Tests use [`metrics::with_local_recorder`] so emission goes to a [`metrics_util::debugging::DebuggingRecorder`]
/// without installing a global `metrics` recorder. See <https://docs.rs/metrics/0.24.3/metrics/index.html#local-recorder>.
#[cfg(test)]
mod tests {
    use super::{StreamsMetricsRecorder, PIPELINE_METRIC_PREFIX};
    use metrics::{with_local_recorder, Key, Label};
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use metrics_util::{CompositeKey, MetricKind};
    use ordered_float::OrderedFloat;

    fn expected_key(name: &str, labels: Vec<Label>) -> CompositeKey {
        CompositeKey::new(
            MetricKind::Counter,
            Key::from_parts(format!("{PIPELINE_METRIC_PREFIX}.{name}"), labels),
        )
    }

    fn expected_key_hist(name: &str, labels: Vec<Label>) -> CompositeKey {
        CompositeKey::new(
            MetricKind::Histogram,
            Key::from_parts(format!("{PIPELINE_METRIC_PREFIX}.{name}"), labels),
        )
    }

    fn expected_key_gauge(name: &str, labels: Vec<Label>) -> CompositeKey {
        CompositeKey::new(
            MetricKind::Gauge,
            Key::from_parts(format!("{PIPELINE_METRIC_PREFIX}.{name}"), labels),
        )
    }

    #[test]
    fn streams_recorder_emits_prefixed_counter_histogram_and_gauge() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let sr = StreamsMetricsRecorder;

        with_local_recorder(&recorder, || {
            sr.record_counter(
                "send_backpressure",
                &[("step", "map_a"), ("reason", "full")],
                3,
            );
            sr.record_histogram("receive_backpressure_duration", &[("step", "sink_1")], 42.5);
            sr.record_gauge("queue_depth", &[("shard", "0")], 17.0);
        });

        let map = snapshotter.snapshot().into_hashmap();

        assert_eq!(
            map.get(&expected_key(
                "send_backpressure",
                vec![Label::new("step", "map_a"), Label::new("reason", "full")],
            )),
            Some(&(None, None, DebugValue::Counter(3)))
        );
        assert_eq!(
            map.get(&expected_key_hist(
                "receive_backpressure_duration",
                vec![Label::new("step", "sink_1")],
            )),
            Some(&(None, None, DebugValue::Histogram(vec![OrderedFloat(42.5)])))
        );
        assert_eq!(
            map.get(&expected_key_gauge(
                "queue_depth",
                vec![Label::new("shard", "0")]
            )),
            Some(&(None, None, DebugValue::Gauge(OrderedFloat(17.0))))
        );
    }
}
