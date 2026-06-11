//! Arroyo processing strategy that delegates message processing to a WASM guest
//! component via wasmtime.

use crate::messages::{
    into_pyraw, PyStreamingMessage, RawMessage, RoutedValuePayload, Watermark, WatermarkMessage,
};
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use anyhow::{Context, Result};
use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, ProcessingStrategy, StrategyError, SubmitError,
};
use sentry_arroyo::types::{Message, Partition, Topic};
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{debug, error, info, warn};
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxBuilder, WasiView};

wasmtime::component::bindgen!({
    world: "plugin",
    path: "wit",
    async: false,
});

struct HostCtx {
    table: ResourceTable,
    wasi: WasiCtx,
    step_name: String,
}

impl WasiView for HostCtx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }

    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

impl sentry_streams::processor::log::Host for HostCtx {
    fn debug(&mut self, message: String) {
        debug!(step = %self.step_name, "{message}");
    }

    fn info(&mut self, message: String) {
        info!(step = %self.step_name, "{message}");
    }

    fn warn(&mut self, message: String) {
        warn!(step = %self.step_name, "{message}");
    }

    fn error(&mut self, message: String) {
        error!(step = %self.step_name, "{message}");
    }
}

type WitMessage = exports::sentry_streams::processor::processor::Message;
type WitWatermark = exports::sentry_streams::processor::processor::Watermark;
type WitOutput = exports::sentry_streams::processor::processor::Output;

fn committable_to_wit(committable: &BTreeMap<Partition, u64>) -> Vec<((String, u64), u64)> {
    committable
        .iter()
        .map(|(partition, offset)| {
            (
                (partition.topic.as_str().to_string(), partition.index as u64),
                *offset,
            )
        })
        .collect()
}

fn committable_from_wit(committable: &[((String, u64), u64)]) -> BTreeMap<Partition, u64> {
    committable
        .iter()
        .map(|((topic, index), offset)| {
            (
                Partition {
                    topic: Topic::new(topic),
                    index: *index as u16,
                },
                *offset,
            )
        })
        .collect()
}

fn watermark_to_wit(watermark: &Watermark) -> WitWatermark {
    WitWatermark {
        committable: committable_to_wit(&watermark.committable),
        timestamp: watermark.timestamp as f64,
        last_message_time: watermark.last_message_time.unwrap_or(0.0),
    }
}

fn wit_watermark_to_rust(wm: &WitWatermark) -> Watermark {
    Watermark::with_last_message_time(
        committable_from_wit(&wm.committable),
        wm.timestamp as u64,
        Some(wm.last_message_time),
    )
}

fn extract_wit_message(payload: &RoutedValuePayload) -> Result<WitMessage> {
    match payload {
        RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::RawMessage { content }) => {
            traced_with_gil!(|py| {
                let raw = content.bind(py).borrow();
                Ok(WitMessage {
                    headers: raw.headers.clone(),
                    timestamp: raw.timestamp,
                    payload: raw.payload.clone(),
                })
            })
        }
        RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::PyAnyMessage { content }) => {
            traced_with_gil!(|py| {
                let bound = content.bind(py);
                let payload = bound.borrow();
                let bytes: Vec<u8> = match payload.payload.extract::<Vec<u8>>(py) {
                    Ok(b) => b,
                    Err(_) => match payload.payload.extract::<&[u8]>(py) {
                        Ok(b) => b.to_vec(),
                        Err(e) => {
                            return Err(anyhow::anyhow!(
                                "WasmProcessor requires bytes payload, got non-bytes PyAnyMessage: {e}"
                            ));
                        }
                    },
                };
                Ok(WitMessage {
                    headers: payload.headers.clone(),
                    timestamp: payload.timestamp,
                    payload: bytes,
                })
            })
        }
        RoutedValuePayload::WatermarkMessage(..) => {
            anyhow::bail!("expected streaming message, got watermark");
        }
    }
}

fn wit_message_to_routed(route: &Route, msg: &WitMessage, schema: Option<String>) -> RoutedValue {
    let raw = RawMessage {
        payload: msg.payload.clone(),
        headers: msg.headers.clone(),
        timestamp: msg.timestamp,
        schema,
    };
    let py_msg = traced_with_gil!(|py| {
        PyStreamingMessage::RawMessage {
            content: into_pyraw(py, raw).expect("into_pyraw"),
        }
    });
    RoutedValue {
        route: route.clone(),
        payload: RoutedValuePayload::PyStreamingMessage(py_msg),
    }
}

fn wit_output_to_message(
    route: &Route,
    output: WitOutput,
    schema: Option<String>,
) -> Message<RoutedValue> {
    match output {
        WitOutput::Msg(msg) => {
            let routed = wit_message_to_routed(route, &msg, schema);
            Message::new_any_message(routed, BTreeMap::new())
        }
        WitOutput::Wm(wm) => {
            let watermark = wit_watermark_to_rust(&wm);
            let committable = watermark.committable.clone();
            let routed = RoutedValue {
                route: route.clone(),
                payload: RoutedValuePayload::WatermarkMessage(WatermarkMessage::Watermark(
                    watermark,
                )),
            };
            Message::new_any_message(routed, committable)
        }
    }
}

struct WasmGuest {
    store: Store<HostCtx>,
    bindings: Plugin,
}

impl WasmGuest {
    fn new(module_path: &str, step_name: &str) -> Result<Self> {
        let path = Path::new(module_path);
        if !path.exists() {
            anyhow::bail!("WASM module not found at {}", module_path);
        }

        let mut config = Config::new();
        config.wasm_component_model(true);
        let engine = Engine::new(&config)?;

        let component = Component::from_file(&engine, path)
            .with_context(|| format!("loading WASM component from {}", module_path))?;

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::add_to_linker_sync(&mut linker).context("linking WASI Preview 2")?;
        Plugin::add_to_linker(&mut linker, |state: &mut HostCtx| state)
            .context("linking plugin host imports")?;

        let wasi = WasiCtxBuilder::new().build();
        let host = HostCtx {
            table: ResourceTable::new(),
            wasi,
            step_name: step_name.to_string(),
        };
        let mut store = Store::new(&engine, host);
        let bindings = Plugin::instantiate(&mut store, &component, &linker)
            .context("instantiating WASM plugin")?;

        Ok(Self { store, bindings })
    }

    fn call_submit(&mut self, msg: WitMessage) -> Result<()> {
        self.bindings
            .sentry_streams_processor_processor()
            .call_submit(&mut self.store, &msg)
            .context("guest submit")?;
        Ok(())
    }

    fn call_submit_watermark(&mut self, wm: WitWatermark) -> Result<()> {
        self.bindings
            .sentry_streams_processor_processor()
            .call_submit_watermark(&mut self.store, &wm)
            .context("guest submit_watermark")?;
        Ok(())
    }

    fn call_poll(&mut self) -> Result<Vec<WitOutput>> {
        let outputs = self
            .bindings
            .sentry_streams_processor_processor()
            .call_poll(&mut self.store)
            .context("guest poll")?;
        Ok(outputs.unwrap_or_default())
    }
}

/// Single-threaded WASM processor strategy.
pub struct WasmProcessor {
    route: Route,
    schema: Option<String>,
    guest: Mutex<WasmGuest>,
    next_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,
    pending: VecDeque<Message<RoutedValue>>,
    commit_request_carried_over: Option<CommitRequest>,
}

impl WasmProcessor {
    pub fn new(
        route: Route,
        module_path: String,
        next_strategy: Box<dyn ProcessingStrategy<RoutedValue>>,
    ) -> Self {
        let step_name = route
            .waypoints
            .last()
            .cloned()
            .unwrap_or_else(|| route.source.clone());
        let guest = WasmGuest::new(&module_path, &step_name)
            .unwrap_or_else(|e| panic!("failed to initialize WASM guest: {e:#}"));
        Self {
            route,
            schema: None,
            guest: Mutex::new(guest),
            next_strategy,
            pending: VecDeque::new(),
            commit_request_carried_over: None,
        }
    }

    fn forward_pending(&mut self) -> Result<(), StrategyError> {
        while let Some(msg) = self.pending.pop_front() {
            let commit_request = self.next_strategy.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), commit_request);
            match self.next_strategy.submit(msg) {
                Err(SubmitError::MessageRejected(
                    sentry_arroyo::processing::strategies::MessageRejected { message },
                )) => {
                    self.pending.push_front(message);
                    break;
                }
                Err(SubmitError::InvalidMessage(invalid_message)) => {
                    return Err(invalid_message.into());
                }
                Ok(()) => {}
            }
        }
        Ok(())
    }

    fn poll_guest_and_forward(&mut self) -> Result<(), StrategyError> {
        let outputs = {
            let mut guest = self
                .guest
                .lock()
                .expect("WasmProcessor guest mutex poisoned");
            guest.call_poll().map_err(|e| {
                StrategyError::Other(Box::new(std::io::Error::other(e.to_string())))
            })?
        };
        for output in outputs {
            let msg = wit_output_to_message(&self.route, output, self.schema.clone());
            self.pending.push_back(msg);
        }
        self.forward_pending()
    }
}

impl ProcessingStrategy<RoutedValue> for WasmProcessor {
    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.route != message.payload().route {
            return self.next_strategy.submit(message);
        }

        let payload = &message.payload().payload;
        if let RoutedValuePayload::WatermarkMessage(WatermarkMessage::Watermark(watermark)) =
            payload
        {
            let wit_wm = watermark_to_wit(watermark);
            let mut guest = self
                .guest
                .lock()
                .expect("WasmProcessor guest mutex poisoned");
            if let Err(e) = guest.call_submit_watermark(wit_wm) {
                tracing::error!("WASM guest submit_watermark failed: {e:#}");
                panic!("WASM guest submit_watermark failed: {e:#}");
            }
            return Ok(());
        }
        if let RoutedValuePayload::WatermarkMessage(WatermarkMessage::PyWatermark(..)) = payload {
            panic!("PyWatermark should not be submitted to WasmProcessor");
        }

        let wit_msg = extract_wit_message(payload).unwrap_or_else(|e| {
            tracing::error!("WasmProcessor message conversion failed: {e:#}");
            panic!("WasmProcessor message conversion failed: {e:#}");
        });
        let mut guest = self
            .guest
            .lock()
            .expect("WasmProcessor guest mutex poisoned");
        if let Err(e) = guest.call_submit(wit_msg) {
            tracing::error!("WASM guest submit failed: {e:#}");
            panic!("WASM guest submit failed: {e:#}");
        }
        Ok(())
    }

    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.poll_guest_and_forward()?;
        let commit_request = self.next_strategy.poll()?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            commit_request,
        ))
    }

    fn terminate(&mut self) {
        self.next_strategy.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        self.poll_guest_and_forward()?;
        let commit_request = self.next_strategy.join(timeout)?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            commit_request,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::FakeStrategy;
    use crate::messages::{RoutedValuePayload, Watermark};
    use crate::testutils::{build_routed_value, make_committable};
    use pyo3::IntoPyObjectExt;
    use std::sync::{Arc, Mutex};

    fn fixture_path() -> Option<String> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/wasm_passthrough.wasm");
        if path.exists() {
            Some(path.to_string_lossy().into_owned())
        } else {
            None
        }
    }

    fn make_test_watermark(route: Route) -> Message<RoutedValue> {
        let committable = make_committable(2, 0);
        let watermark = Watermark::new(committable.clone(), 42);
        let routed_watermark = RoutedValue {
            route,
            payload: RoutedValuePayload::WatermarkMessage(WatermarkMessage::Watermark(watermark)),
        };
        Message::new_any_message(routed_watermark, committable)
    }

    #[test]
    fn test_wasm_passthrough_message() {
        let Some(module_path) = fixture_path() else {
            eprintln!("skipping test_wasm_passthrough_message: fixture not built");
            return;
        };
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let submitted_messages = Arc::new(Mutex::new(Vec::new()));
            let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
            let next_step = FakeStrategy::new(
                submitted_messages.clone(),
                submitted_watermarks.clone(),
                false,
            );

            let route = Route::new("source1".to_string(), vec!["wasm".to_string()]);
            let mut processor =
                WasmProcessor::new(route.clone(), module_path, Box::new(next_step));

            let routed = build_routed_value(
                py,
                b"hello".to_vec().into_py_any(py).unwrap(),
                "source1",
                vec!["wasm".to_string()],
            );
            let committable = make_committable(1, 0);
            let message = Message::new_any_message(routed, committable);
            processor.submit(message).unwrap();
            processor.poll().unwrap();

            let messages = submitted_messages.lock().unwrap();
            assert_eq!(messages.len(), 1);
        });
    }

    #[test]
    fn test_wasm_passthrough_watermark() {
        let Some(module_path) = fixture_path() else {
            eprintln!("skipping test_wasm_passthrough_watermark: fixture not built");
            return;
        };
        crate::testutils::initialize_python();
        let submitted_messages = Arc::new(Mutex::new(Vec::new()));
        let submitted_watermarks = Arc::new(Mutex::new(Vec::new()));
        let next_step = FakeStrategy::new(submitted_messages, submitted_watermarks.clone(), false);

        let route = Route::new("source1".to_string(), vec!["wasm".to_string()]);
        let mut processor = WasmProcessor::new(route.clone(), module_path, Box::new(next_step));

        let watermark = make_test_watermark(route);
        processor.submit(watermark).unwrap();
        processor.poll().unwrap();

        assert!(!submitted_watermarks.lock().unwrap().is_empty());
    }
}
