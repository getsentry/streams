//! The Arrow batch parser step: batches raw Kafka payloads and decodes them into
//! an Apache Arrow `RecordBatch` without ever materialising the individual
//! messages as Python objects.
//!
//! The batch leaves as an Arrow IPC stream in a `RawMessage`, so Python receives
//! ordinary `bytes` and reads them with `polars.read_ipc_stream`. That costs a
//! serialization and a copy into Python memory; handing the `RecordBatch` over
//! directly through the Arrow C data interface is deferred.
//!
//! It reuses [`BatchStep`] wholesale -- windowing, watermark ordering and
//! backpressure are identical to the `Batch` step -- and supplies its own
//! [`BatchFlushProducer`]. See `docs/design/arrow-batch-parser.md`, phase 4.

use crate::batch_step::{BatchElement, BatchFlushProducer, BatchStep};
use crate::extractors::{get_extractor, registered_resources, Extractor};
use crate::messages::{into_pyraw, PyStreamingMessage, RawMessage, RoutedValuePayload};
use crate::routes::{Route, RoutedValue};
use crate::utils::traced_with_gil;
use arrow::array::RecordBatch;
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::{ProcessingStrategy, StrategyError};
use sentry_arroyo::types::{Message, Partition};
use sentry_kafka_schemas::{get_schema, SchemaType};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Runs `f` over the batch's payload bytes.
///
/// **This is the only place that knows where payload bytes live.** Today the
/// source boxes every payload into a `Py<RawMessage>` (`consumer.rs`), so the
/// bytes are Python-owned and reading them needs the GIL; the borrow is held for
/// the duration of the decode. When the source starts emitting Rust-native
/// messages this function loses its GIL block and its `PyRef` guards, and
/// nothing else in the step changes.
///
/// It is a scope rather than a plain accessor because the `PyRef` guards must
/// outlive the slices handed to `f`.
///
/// Do **not** copy the payloads out to release the GIL sooner. It would work
/// today and would become permanent dead weight the moment the source goes
/// native -- a per-message copy in the one step whose whole purpose is to remove
/// per-message copies.
fn with_payloads<R>(
    step_name: &str,
    elements: &[BatchElement],
    f: impl FnOnce(&[&[u8]]) -> R,
) -> R {
    traced_with_gil!(|py| {
        let guards: Vec<PyRef<'_, crate::messages::RawMessage>> = elements
            .iter()
            .map(|element| match element {
                PyStreamingMessage::RawMessage { content } => content.bind(py).borrow(),
                // Decision 10: this step reads bytes off the wire. A PyAnyMessage
                // means a Python step ran in between and the bytes are gone.
                // The adapter rejects this at build time; this is the backstop.
                PyStreamingMessage::PyAnyMessage { .. } => panic!(
                    "step '{step_name}': the Arrow batch parser only accepts raw messages, \
                     but the window contains a message already converted to a Python object. \
                     Place this step directly after the source."
                ),
            })
            .collect();

        let payloads: Vec<&[u8]> = guards.iter().map(|g| g.payload.as_slice()).collect();
        f(&payloads)
    })
}

/// Serialize a batch as an Arrow IPC stream.
///
/// This costs a copy into a `Vec<u8>` and another into Python memory. That is
/// deliberate for now: it keeps the handoff an ordinary `bytes` payload, which
/// every existing step already understands, rather than an FFI object. Python
/// reads it back with `polars.read_ipc_stream`.
fn to_ipc_stream(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buffer, batch.schema().as_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    drop(writer);
    Ok(buffer)
}

/// Decodes a flushed window into a `RecordBatch` and hands it to Python.
pub(crate) struct ArrowFlushProducer {
    extractor: &'static dyn Extractor,
    step_name: String,
    schema_name: String,
}

impl ArrowFlushProducer {
    /// Resolve topic -> schema -> extractor once, at step construction.
    ///
    /// Every failure here is a configuration error that will never fix itself at
    /// runtime, so each one panics at startup rather than at the first message.
    /// `get_schema` leaks on protobuf topics and must never be called per
    /// message, which is the other reason this happens exactly once.
    fn resolve(step_name: String, schema_name: &str) -> Self {
        let schema = get_schema(schema_name, None).unwrap_or_else(|e| {
            panic!(
                "step '{step_name}': no schema registered for topic '{schema_name}': {e}. \
                 The Arrow batch parser resolves its extractor from the source topic's schema."
            )
        });

        if schema.schema_type != SchemaType::Protobuf {
            panic!(
                "step '{step_name}': topic '{schema_name}' has schema type {:?}, but the Arrow \
                 batch parser supports protobuf only.",
                schema.schema_type
            );
        }

        let resource = schema.raw_schema();
        let extractor = get_extractor(resource).unwrap_or_else(|| {
            panic!(
                "step '{step_name}': topic '{schema_name}' carries '{resource}', which has no \
                 Arrow extractor. Known message types: {:?}",
                registered_resources()
            )
        });

        Self {
            extractor,
            step_name,
            schema_name: schema_name.to_string(),
        }
    }
}

impl BatchFlushProducer for ArrowFlushProducer {
    fn produce(
        &self,
        route: &Route,
        elements: &[BatchElement],
        committable: BTreeMap<Partition, u64>,
    ) -> Result<Message<RoutedValue>, StrategyError> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let batch = with_payloads(&self.step_name, elements, |payloads| {
            self.extractor.extract(payloads)
        })
        .unwrap_or_else(|e| {
            // Offsets are collapsed to max per partition on flush, so there is no
            // (partition, offset) for arroyo's DLQ to reject a single row with,
            // and no way to fail the batch without failing the window. Panicking
            // matches what the runtime already does for an error on an
            // AnyMessage; see transformer.rs.
            panic!(
                "step '{}': could not decode a batch of {} message(s) from topic '{}': {e}",
                self.step_name,
                elements.len(),
                self.schema_name,
            )
        });

        let payload = to_ipc_stream(&batch).unwrap_or_else(|e| {
            panic!(
                "step '{}': could not serialize a {}-row Arrow batch from topic '{}': {e}",
                self.step_name,
                batch.num_rows(),
                self.schema_name,
            )
        });

        let content = traced_with_gil!(|py| {
            into_pyraw(
                py,
                RawMessage {
                    payload,
                    headers: vec![],
                    timestamp: ts,
                    // Deliberately not `schema_name`. In this runtime `schema` means
                    // "the schema this payload can be decoded with" (see
                    // `msg_codecs._get_codec_from_msg`), and these bytes are an Arrow
                    // IPC stream, not a message of the source's schema.
                    schema: None,
                },
            )
        })
        .map_err(|e| StrategyError::Other(Box::new(e)))?;

        Ok(Message::new_any_message(
            RoutedValue {
                route: route.clone(),
                payload: RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::RawMessage {
                    content,
                }),
            },
            committable,
        ))
    }
}

pub fn build_arrow_batch_parser_step(
    route: &Route,
    schema_name: &str,
    step_name: String,
    max_batch_size: Option<usize>,
    max_batch_time: Option<Duration>,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    let producer = ArrowFlushProducer::resolve(step_name.clone(), schema_name);
    Box::new(BatchStep::with_producer(
        route.clone(),
        max_batch_size,
        max_batch_time,
        step_name,
        next,
        Arc::new(producer),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_strategy::FakeStrategy;
    use crate::testutils::{build_raw_routed_value, build_routed_value};
    use arrow::array::{StringArray, UInt64Array};
    use arrow::ipc::reader::StreamReader;
    use prost::Message as _;
    use pyo3::types::PyAnyMethods;
    use pyo3::IntoPyObject;
    use sentry_arroyo::types::{Partition, Topic};
    use sentry_protos::snuba::v1::TraceItem;
    use std::sync::Mutex;

    const SNUBA_ITEMS: &str = "snuba-items";

    fn route() -> Route {
        Route::new("s".into(), vec!["w".into()])
    }

    fn trace_item(org: u64) -> Vec<u8> {
        TraceItem {
            organization_id: org,
            trace_id: format!("trace-{org}"),
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn producer() -> ArrowFlushProducer {
        ArrowFlushProducer::resolve("test_arrow".to_string(), SNUBA_ITEMS)
    }

    fn from_ipc_stream(bytes: &[u8]) -> RecordBatch {
        let mut reader = StreamReader::try_new(bytes, None).expect("valid Arrow IPC stream");
        let batch = reader.next().expect("one batch").expect("readable batch");
        assert!(reader.next().is_none(), "stream carries exactly one batch");
        batch
    }

    /// Pull the emitted payload apart the way a Python consumer would: raw bytes
    /// out of a `RawMessage`, decoded as an Arrow IPC stream.
    fn batch_of(message: Message<RoutedValue>) -> (RecordBatch, Option<String>) {
        let payload = message.into_payload();
        let content = match payload.payload {
            RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::RawMessage { content }) => {
                content
            }
            _ => panic!("expected a RawMessage carrying the serialized batch"),
        };
        traced_with_gil!(|py| {
            let borrowed = content.bind(py).borrow();
            (from_ipc_stream(&borrowed.payload), borrowed.schema.clone())
        })
    }

    #[test]
    fn decodes_a_window_of_raw_messages_into_one_record_batch() {
        crate::testutils::initialize_python();
        let partition = Partition::new(Topic::new("t"), 0);
        let committable = BTreeMap::from([(partition, 11_u64)]);

        let (batch, schema) = traced_with_gil!(|py| {
            let elements: Vec<BatchElement> = [1_u64, 2, 3]
                .into_iter()
                .map(|org| {
                    match build_raw_routed_value(py, trace_item(org), "s", vec!["w".into()]).payload
                    {
                        RoutedValuePayload::PyStreamingMessage(m) => m,
                        _ => unreachable!(),
                    }
                })
                .collect();

            let message = producer()
                .produce(&route(), &elements, committable.clone())
                .expect("produce");
            assert_eq!(
                message.committable().collect::<BTreeMap<_, _>>(),
                committable
            );
            batch_of(message)
        });

        assert_eq!(batch.num_rows(), 3);
        let orgs = batch
            .column_by_name("organization_id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(orgs.values(), &[1, 2, 3]);
        let traces = batch
            .column_by_name("trace_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(traces.value(0), "trace-1");

        // `schema` means "decodable with this codec" in this runtime, and an Arrow
        // IPC stream is not a snuba-items message, so it is deliberately unset.
        assert_eq!(schema, None);
    }

    /// Decision 10: this step only accepts RawMessage. A PyAnyMessage means some
    /// Python step ran in between and the payload bytes are gone.
    #[test]
    #[should_panic(expected = "test_arrow")]
    fn a_python_payload_in_the_window_panics() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let payload = 1i32.into_pyobject(py).unwrap().into_any().unbind();
            let element = match build_routed_value(py, payload, "s", vec!["w".into()]).payload {
                RoutedValuePayload::PyStreamingMessage(m) => m,
                _ => unreachable!(),
            };
            let _ = producer().produce(&route(), &[element], BTreeMap::new());
        });
    }

    /// A malformed payload cannot be dead-lettered -- offsets are collapsed to
    /// max per partition -- so it fails the process. Decisions 9 and 14.
    #[test]
    #[should_panic(expected = "row 1")]
    fn a_malformed_payload_panics_naming_the_row() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let payloads = vec![trace_item(1), vec![0xff, 0xff, 0xff]];
            let elements: Vec<BatchElement> = payloads
                .into_iter()
                .map(
                    |p| match build_raw_routed_value(py, p, "s", vec!["w".into()]).payload {
                        RoutedValuePayload::PyStreamingMessage(m) => m,
                        _ => unreachable!(),
                    },
                )
                .collect();
            let _ = producer().produce(&route(), &elements, BTreeMap::new());
        });
    }

    #[test]
    #[should_panic(expected = "not-a-real-topic")]
    fn an_unknown_topic_panics_at_construction() {
        ArrowFlushProducer::resolve("test_arrow".to_string(), "not-a-real-topic");
    }

    /// JSON topics are out of scope for the PoC and must fail loudly at startup
    /// rather than at the first message.
    #[test]
    #[should_panic(expected = "Json")]
    fn a_json_topic_panics_at_construction() {
        let topic = "events";
        let schema = sentry_kafka_schemas::get_schema(topic, None).unwrap();
        assert_eq!(
            schema.schema_type,
            SchemaType::Json,
            "{topic} is expected to be a JSON topic"
        );
        ArrowFlushProducer::resolve("test_arrow".to_string(), topic);
    }

    /// The step is a BatchStep underneath: same windowing, same watermarks.
    #[test]
    fn behaves_as_a_batch_step_end_to_end() {
        crate::testutils::initialize_python();
        let sub = Arc::new(Mutex::new(Vec::new()));
        let wms = Arc::new(Mutex::new(Vec::new()));
        let mut step = build_arrow_batch_parser_step(
            &route(),
            SNUBA_ITEMS,
            "test_arrow".to_string(),
            Some(2),
            None,
            Box::new(FakeStrategy::new(sub.clone(), wms, false)),
        );

        traced_with_gil!(|py| {
            for org in [1_u64, 2] {
                let msg = Message::new_any_message(
                    build_raw_routed_value(py, trace_item(org), "s", vec!["w".into()]),
                    BTreeMap::new(),
                );
                step.submit(msg).unwrap();
            }
            step.poll().unwrap();
        });

        let out = sub.lock().unwrap();
        assert_eq!(out.len(), 1, "one batch downstream, not two rows");
        traced_with_gil!(|py| {
            let bytes: Vec<u8> = out[0].bind(py).extract().unwrap();
            assert_eq!(from_ipc_stream(&bytes).num_rows(), 2);
        });
    }

    /// An empty window never reaches a producer, but the extractor's empty batch
    /// must still be schema-correct if it ever does.
    #[test]
    fn an_empty_window_still_produces_a_typed_batch() {
        crate::testutils::initialize_python();
        let message = producer()
            .produce(&route(), &[], BTreeMap::new())
            .expect("produce");
        let (batch, _) = batch_of(message);
        assert_eq!(batch.num_rows(), 0);
        assert!(batch.num_columns() > 0);
    }

    /// The whole point of the exercise, verified end to end: raw protobuf off the
    /// wire reaches polars as a typed frame without a Python object per message.
    #[test]
    fn a_python_consumer_reads_the_batch_with_polars() {
        crate::testutils::initialize_python();
        traced_with_gil!(|py| {
            let items: Vec<TraceItem> = (1..=4)
                .map(|org| TraceItem {
                    organization_id: org,
                    trace_id: format!("trace-{org}"),
                    item_type: 1,
                    attributes: std::collections::HashMap::from([(
                        "service".to_string(),
                        sentry_protos::snuba::v1::AnyValue {
                            value: Some(sentry_protos::snuba::v1::any_value::Value::StringValue(
                                format!("svc-{org}"),
                            )),
                        },
                    )]),
                    ..Default::default()
                })
                .collect();

            let elements: Vec<BatchElement> = items
                .iter()
                .map(|item| {
                    match build_raw_routed_value(py, item.encode_to_vec(), "s", vec!["w".into()])
                        .payload
                    {
                        RoutedValuePayload::PyStreamingMessage(m) => m,
                        _ => unreachable!(),
                    }
                })
                .collect();

            let message = producer()
                .produce(&route(), &elements, BTreeMap::new())
                .expect("produce");

            let payload = message.into_payload();
            let content = match payload.payload {
                RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::RawMessage {
                    content,
                }) => content,
                _ => unreachable!(),
            };
            let py_bytes = content.bind(py).getattr("payload").unwrap();

            // Exactly what a downstream Map would do with the payload.
            let pl = py.import("polars").expect("polars must be importable");
            let df = pl.call_method1("read_ipc_stream", (py_bytes,)).unwrap();

            assert_eq!(
                df.call_method0("__len__")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                4
            );
            let trace_ids: Vec<String> = df
                .get_item("trace_id")
                .unwrap()
                .call_method0("to_list")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(trace_ids, vec!["trace-1", "trace-2", "trace-3", "trace-4"]);

            let item_types: Vec<String> = df
                .get_item("item_type")
                .unwrap()
                .call_method0("to_list")
                .unwrap()
                .extract()
                .unwrap();
            assert!(item_types.iter().all(|t| t == "TRACE_ITEM_TYPE_SPAN"));

            // The type-split attribute map survives the FFI boundary as a nested
            // column rather than being flattened or dropped.
            let columns: Vec<String> = df.getattr("columns").unwrap().extract().unwrap();
            assert!(columns.contains(&"attr_str".to_string()), "{columns:?}");
            let dtype = df
                .get_item("attr_str")
                .unwrap()
                .getattr("dtype")
                .unwrap()
                .str()
                .unwrap()
                .extract::<String>()
                .unwrap();
            assert!(
                dtype.contains("List") || dtype.contains("Struct"),
                "attr_str should arrive as a nested column, got {dtype}"
            );
        });
    }

    /// Phase 6's benchmark. Not run by default -- it is a measurement, not an
    /// assertion:
    ///
    ///     cargo test --release bench_arrow_vs_pylist -- --ignored --nocapture
    ///
    /// It compares the Arrow producer against the Python-list producer the
    /// existing `Batch` step uses, on the same window, and so answers whether
    /// inline decoding on the consumer thread is affordable.
    #[test]
    #[ignore = "benchmark: run explicitly with --ignored --nocapture"]
    fn bench_arrow_vs_pylist() {
        use crate::batch_step::PyListFlushProducer;
        use std::time::Instant;

        crate::testutils::initialize_python();
        const ROWS: usize = 10_000;
        const REPEATS: usize = 20;

        traced_with_gil!(|py| {
            let elements: Vec<BatchElement> = (0..ROWS)
                .map(|i| {
                    let item = TraceItem {
                        organization_id: i as u64,
                        trace_id: format!("trace-{i}"),
                        item_type: 1,
                        attributes: std::collections::HashMap::from([(
                            "service".to_string(),
                            sentry_protos::snuba::v1::AnyValue {
                                value: Some(
                                    sentry_protos::snuba::v1::any_value::Value::StringValue(
                                        "checkout".to_string(),
                                    ),
                                ),
                            },
                        )]),
                        ..Default::default()
                    };
                    match build_raw_routed_value(py, item.encode_to_vec(), "s", vec!["w".into()])
                        .payload
                    {
                        RoutedValuePayload::PyStreamingMessage(m) => m,
                        _ => unreachable!(),
                    }
                })
                .collect();

            let arrow = producer();
            let pylist = PyListFlushProducer;

            // The path this step replaces: Batch -> Map(extract_bytes) -> BatchParser.
            // Decoding in Python is what makes it a fair comparison; the list build
            // alone is not the competitor.
            let decode_in_python = py
                .eval(
                    c"lambda batch, codec: [codec.decode(p, validate=False) for p in batch]",
                    None,
                    None,
                )
                .unwrap();
            let codec = py
                .import("sentry_kafka_schemas")
                .unwrap()
                .call_method1("get_codec", ("snuba-items",))
                .unwrap();

            let mut arrow_times = Vec::with_capacity(REPEATS);
            let mut pylist_times = Vec::with_capacity(REPEATS);
            let mut python_times = Vec::with_capacity(REPEATS);
            for _ in 0..REPEATS {
                let t = Instant::now();
                arrow
                    .produce(&route(), &elements, BTreeMap::new())
                    .expect("arrow produce");
                arrow_times.push(t.elapsed().as_secs_f64());

                let t = Instant::now();
                let listed = pylist
                    .produce(&route(), &elements, BTreeMap::new())
                    .expect("pylist produce");
                pylist_times.push(t.elapsed().as_secs_f64());

                let batch_list = match listed.into_payload().payload {
                    RoutedValuePayload::PyStreamingMessage(PyStreamingMessage::PyAnyMessage {
                        content,
                    }) => content.bind(py).borrow().payload.clone_ref(py),
                    _ => unreachable!(),
                };
                let t = Instant::now();
                decode_in_python
                    .call1((batch_list, &codec))
                    .expect("python decode");
                python_times.push(t.elapsed().as_secs_f64() + pylist_times[pylist_times.len() - 1]);
            }

            let report = |label: &str, mut times: Vec<f64>| {
                times.sort_by(|a, b| a.partial_cmp(b).unwrap());
                let p50 = times[times.len() / 2];
                let p99 = times[(times.len() as f64 * 0.99) as usize % times.len()];
                println!(
                    "{label:>10}: p50 {:>8.2}ms  p99 {:>8.2}ms  {:>10.0} rows/s",
                    p50 * 1000.0,
                    p99 * 1000.0,
                    ROWS as f64 / p50
                );
            };

            println!("\n{ROWS} rows/window, {REPEATS} windows");
            report("arrow", arrow_times);
            report("list only", pylist_times);
            report("list+parse", python_times);
            println!(
                "  arrow     = ArrowBatchParser: decode to a RecordBatch in Rust\n  \
                 list only = Batch's flush alone, a Python list of `bytes` (not a \
                 complete path)\n  list+parse = Batch -> BatchParser, the path this \
                 step replaces\n"
            );
        });
    }
}
