use crate::kafka_config::PyMetricConfig;
use sentry_arroyo::metrics::{init, MetricSink, StatsdRecorder};
use std::net::UdpSocket;

/// A simple UDP sink for sending StatsD metrics
struct UdpMetricSink {
    socket: UdpSocket,
    addr: String,
}

impl UdpMetricSink {
    fn new(host: &str, port: u16) -> Result<Self, std::io::Error> {
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        let addr = format!("{}:{}", host, port);
        Ok(Self { socket, addr })
    }
}

impl MetricSink for UdpMetricSink {
    fn emit(&self, metric: &str) {
        if let Err(e) = self.socket.send_to(metric.as_bytes(), &self.addr) {
            eprintln!("Failed to send metric: {}", e);
        }
    }
}

// Initialize metrics if config is provided
pub fn configure_metrics(metric_config: Option<PyMetricConfig>) {
    if let Some(ref metric_config) = metric_config {
        println!(
            "Initializing metrics with host: {}:{}",
            metric_config.datadog_host(),
            metric_config.datadog_port()
        );

        match UdpMetricSink::new(metric_config.datadog_host(), metric_config.datadog_port()) {
            Ok(sink) => {
                let mut recorder = StatsdRecorder::new("arroyo", sink);

                // Add any global tags from the config
                if let Some(tags) = metric_config.datadog_tags() {
                    for (key, value) in tags {
                        recorder = recorder.with_tag(key, value);
                    }
                }

                if let Err(_) = init(recorder) {
                    println!("Warning: Metrics recorder already initialized, skipping");
                } else {
                    println!("Successfully initialized arroyo metrics");
                }
            }
            Err(e) => {
                println!("Failed to initialize metrics sink: {}", e);
            }
        }
    }
}
