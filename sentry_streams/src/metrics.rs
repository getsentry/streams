use crate::metrics_config::PyMetricConfig;
use sentry_arroyo::metrics::{init, MetricSink, StatsdRecorder};
use std::net::UdpSocket;
use tracing::{error, info, warn};

/// A simple UDP sink for sending StatsD metrics
struct UdpMetricSink {
    socket: UdpSocket,
    addr: String,
}

impl UdpMetricSink {
    fn new(host: &str, port: u16) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        let addr = format!("{}:{}", host, port);
        Ok(Self { socket, addr })
    }
}

impl MetricSink for UdpMetricSink {
    fn emit(&self, metric: &str) {
        if let Err(e) = self.socket.send_to(metric.as_bytes(), &self.addr) {
            error!("Failed to send metric: {}", e);
        }
    }
}

// Initialize metrics if config is provided
pub fn configure_metrics(metric_config: Option<PyMetricConfig>) {
    if let Some(ref metric_config) = metric_config {
        info!(
            "Initializing metrics with host: {}:{}",
            metric_config.host(),
            metric_config.port()
        );

        match UdpMetricSink::new(metric_config.host(), metric_config.port()) {
            Ok(sink) => {
                let mut recorder = StatsdRecorder::new("arroyo", sink);

                // Add any global tags from the config
                if let Some(tags) = metric_config.tags() {
                    for (key, value) in tags {
                        recorder = recorder.with_tag(key, value);
                    }
                }

                if init(recorder).is_err() {
                    warn!("Warning: Metrics recorder already initialized, skipping");
                } else {
                    info!("Successfully initialized arroyo metrics");
                }
            }
            Err(e) => {
                error!("Failed to initialize metrics sink: {}", e);
            }
        }
    }
}
