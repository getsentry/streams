use crate::metrics_config::PyMetricConfig;
use metrics_exporter_dogstatsd::DogStatsDBuilder;
use sentry_arroyo::metrics::{init as arroyo_init, Metric, MetricType, Recorder};
use tracing::{error, info, warn};

struct MetricsFacadeRecorder;

impl Recorder for MetricsFacadeRecorder {
    fn record_metric(&self, metric: Metric<'_>) {
        let key = format!("{}", metric.key);
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

        let builder = DogStatsDBuilder::default()
            .with_remote_address(&format!("{}:{}", host, port))
            .expect("Failed to parse address");

        // Note: DogStatsDBuilder doesn't have with_default_tag like StatsdBuilder
        // Tags would need to be handled differently if required
        if let Some(_tags) = metric_config.tags() {
            info!("Note: DogStatsD tags configuration is not directly supported by the builder");
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

        if arroyo_init(MetricsFacadeRecorder).is_err() {
            warn!("Arroyo metrics recorder already initialized, skipping");
            return;
        }

        info!("Successfully initialized arroyo metrics");
    }
}
