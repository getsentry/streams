use crate::metrics_config::PyMetricConfig;
use metrics::{
    Counter, Gauge, Histogram, Key, KeyName, Label, Metadata, Recorder, SharedString, Unit,
};
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use std::time::Duration;
use tracing::{error, info};

const ARROYO_METRIC_PREFIX: &str = "arroyo.";
const STREAMS_METRIC_PREFIX: &str = "streams.pipeline.";

/// Arroyo emits its own metrics (`arroyo.*`) through the global metrics-rs
/// recorder. This wrapper namespaces them under `streams.pipeline.` so they
/// keep the same names as before and match the Python side, while leaving
/// streams' own metrics (already `streams.pipeline.*`) untouched.
struct ArroyoPrefixRecorder<R> {
    inner: R,
}

fn prefixed_name(name: &str) -> Option<String> {
    name.starts_with(ARROYO_METRIC_PREFIX)
        .then(|| format!("{STREAMS_METRIC_PREFIX}{name}"))
}

fn prefix_key_name(key: KeyName) -> KeyName {
    match prefixed_name(key.as_str()) {
        Some(name) => KeyName::from(name),
        None => key,
    }
}

fn prefix_key(key: &Key) -> Option<Key> {
    prefixed_name(key.name())
        .map(|name| Key::from_parts(name, key.labels().cloned().collect::<Vec<_>>()))
}

impl<R: Recorder> Recorder for ArroyoPrefixRecorder<R> {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner
            .describe_counter(prefix_key_name(key), unit, description)
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner
            .describe_gauge(prefix_key_name(key), unit, description)
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner
            .describe_histogram(prefix_key_name(key), unit, description)
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        match prefix_key(key) {
            Some(key) => self.inner.register_counter(&key, metadata),
            None => self.inner.register_counter(key, metadata),
        }
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        match prefix_key(key) {
            Some(key) => self.inner.register_gauge(&key, metadata),
            None => self.inner.register_gauge(key, metadata),
        }
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        match prefix_key(key) {
            Some(key) => self.inner.register_histogram(&key, metadata),
            None => self.inner.register_histogram(key, metadata),
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

        // Apply global tags if configured
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

        // Apply flush interval if configured
        if let Some(flush_interval_ms) = metric_config.flush_interval_ms() {
            let duration = Duration::from_millis(flush_interval_ms);
            info!("Configuring metrics flush interval to {:?}", duration);
            builder = builder.with_flush_interval(duration);
        }

        let recorder = match builder.build() {
            Ok(recorder) => recorder,
            Err(e) => {
                error!("Failed to build DogStatsD recorder: {}", e);
                return;
            }
        };

        match metrics::set_global_recorder(ArroyoPrefixRecorder { inner: recorder }) {
            Ok(_) => {
                info!("Successfully initialized DogStatsD metrics exporter");
            }
            Err(e) => {
                error!("Failed to install DogStatsD recorder: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefixed_name() {
        assert_eq!(
            prefixed_name("arroyo.consumer.run.count").as_deref(),
            Some("streams.pipeline.arroyo.consumer.run.count")
        );
        assert_eq!(prefixed_name("streams.pipeline.input.messages"), None);
    }

    #[test]
    fn test_prefix_key_keeps_labels() {
        let key = Key::from_parts(
            "arroyo.producer.produce_status",
            vec![Label::new("status", "success")],
        );
        let prefixed = prefix_key(&key).unwrap();
        assert_eq!(
            prefixed.name(),
            "streams.pipeline.arroyo.producer.produce_status"
        );
        assert_eq!(
            prefixed.labels().collect::<Vec<_>>(),
            key.labels().collect::<Vec<_>>()
        );
    }
}
