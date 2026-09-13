//! Extractor for `sentry_protos.snuba.v1.TraceItem`.
//!
//! The Arrow schema is hardcoded here; see `docs/design/arrow-batch-parser.md`
//! for the column-by-column mapping and the reasoning behind it.
//!
//! Two things are worth knowing before changing this file:
//!
//! * **Nullability follows protobuf presence, not the `optional` keyword.**
//!   Message-typed fields (`timestamp`, `received`) always have explicit presence
//!   in proto3, so they are nullable columns. Implicit-presence scalars cannot
//!   distinguish "unset" from "zero", so they are non-nullable columns carrying
//!   the default -- `conversation_id` and `session_id` included, despite their
//!   "if any" comments in the proto.
//! * **`map<string, AnyValue>` is split by value type** into `attr_str`,
//!   `attr_int`, `attr_double`, `attr_bool` and `attr_bytes`. Arrow has no usable
//!   union type here, and Snuba EAP splits the same way. One consequence: an
//!   attribute that changes type between messages lands in different columns.

use crate::extractors::{Extractor, ExtractorError};
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder, MapBuilder,
    MapFieldNames, RecordBatch, StringBuilder, TimestampMicrosecondBuilder, UInt32Builder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use prost::Message;
use prost_types::Timestamp;
use sentry_protos::snuba::v1::{any_value::Value, AnyValue, TraceItem, TraceItemType};
use serde_json::{Map as JsonMap, Value as Json};
use std::sync::{Arc, LazyLock};

pub struct TraceItemExtractor;

pub const RESOURCE: &str = "sentry_protos.snuba.v1.trace_item_pb2.TraceItem";

const TIMESTAMP_TZ: &str = "UTC";

/// Arrow spec names for map entries. arrow-rs defaults to `entries`/`keys`/`values`;
/// the spec (and therefore pyarrow and polars) wants `entries`/`key`/`value`.
/// Getting this wrong breaks interop in confusing ways rather than loudly.
fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    }
}

/// The exact `DataType` a [`MapBuilder`] with [`map_field_names`] produces.
/// Declared explicitly so the schema and the built columns cannot drift apart.
fn map_type(value_type: DataType) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", value_type, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(TIMESTAMP_TZ.into()))
}

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("organization_id", DataType::UInt64, false),
        Field::new("project_id", DataType::UInt64, false),
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("item_id", DataType::Binary, false),
        Field::new("item_type", DataType::Utf8, false),
        Field::new("timestamp", timestamp_type(), true),
        Field::new("client_sample_rate", DataType::Float64, false),
        Field::new("server_sample_rate", DataType::Float64, false),
        Field::new("conversation_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new("retention_days", DataType::UInt32, false),
        Field::new("received", timestamp_type(), true),
        Field::new("downsampled_retention_days", DataType::UInt32, false),
        Field::new("attr_str", map_type(DataType::Utf8), false),
        Field::new("attr_int", map_type(DataType::Int64), false),
        Field::new("attr_double", map_type(DataType::Float64), false),
        Field::new("attr_bool", map_type(DataType::Boolean), false),
        Field::new("attr_bytes", map_type(DataType::Binary), false),
    ]))
});

/// Microseconds since the epoch.
///
/// `checked_*` rather than a silent wrap: a bogus timestamp should be a loud
/// error, not a row that quietly claims to be from the year 1754.
fn timestamp_micros(ts: &Timestamp, field: &'static str) -> Result<i64, ExtractorError> {
    ts.seconds
        .checked_mul(1_000_000)
        .and_then(|s| s.checked_add(i64::from(ts.nanos / 1_000)))
        .ok_or_else(|| ExtractorError::Field {
            field,
            detail: format!(
                "{}s + {}ns overflows microseconds since the epoch",
                ts.seconds, ts.nanos
            ),
        })
}

/// The enum's protobuf name, or a rendered placeholder for a value we do not
/// know about.
///
/// A newer producer adding an enum member is a routine forward-compatible
/// change; panicking on it would turn someone else's deploy into our outage.
fn item_type_name(value: i32) -> String {
    match TraceItemType::try_from(value) {
        Ok(t) => t.as_str_name().to_string(),
        Err(_) => format!("TRACE_ITEM_TYPE_UNKNOWN_{value}"),
    }
}

/// `ArrayValue` and `KeyValueList` are recursive and Arrow has no recursive type,
/// so they are flattened to JSON and stored in `attr_str`.
///
/// Bytes nested inside such a value are base64-encoded, following the proto3
/// canonical JSON mapping. (Top-level `bytes` attributes do *not* go through
/// here: they keep their raw bytes in `attr_bytes`.)
fn any_value_to_json(value: &AnyValue) -> Json {
    match &value.value {
        None => Json::Null,
        Some(Value::StringValue(s)) => Json::String(s.clone()),
        Some(Value::BoolValue(b)) => Json::Bool(*b),
        Some(Value::IntValue(i)) => Json::Number((*i).into()),
        Some(Value::DoubleValue(d)) => serde_json::Number::from_f64(*d)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Some(Value::BytesValue(b)) => Json::String(BASE64.encode(b)),
        Some(Value::ArrayValue(a)) => Json::Array(a.values.iter().map(any_value_to_json).collect()),
        Some(Value::KvlistValue(kv)) => {
            let mut out = JsonMap::with_capacity(kv.values.len());
            for entry in &kv.values {
                let v = entry
                    .value
                    .as_ref()
                    .map(any_value_to_json)
                    .unwrap_or(Json::Null);
                out.insert(entry.key.clone(), v);
            }
            Json::Object(out)
        }
    }
}

/// The five type-split attribute map builders.
struct AttributeBuilders {
    str_: MapBuilder<StringBuilder, StringBuilder>,
    int: MapBuilder<StringBuilder, Int64Builder>,
    double: MapBuilder<StringBuilder, Float64Builder>,
    bool_: MapBuilder<StringBuilder, BooleanBuilder>,
    bytes: MapBuilder<StringBuilder, BinaryBuilder>,
}

impl AttributeBuilders {
    fn new() -> Self {
        Self {
            str_: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            int: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                Int64Builder::new(),
            ),
            double: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                Float64Builder::new(),
            ),
            bool_: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                BooleanBuilder::new(),
            ),
            bytes: MapBuilder::new(
                Some(map_field_names()),
                StringBuilder::new(),
                BinaryBuilder::new(),
            ),
        }
    }

    /// Append one row's attributes.
    ///
    /// Keys are sorted first: prost decodes `map<string, AnyValue>` into a
    /// `HashMap`, whose iteration order varies run to run, and an unsorted batch
    /// would not be reproducible.
    fn append_row(&mut self, item: &TraceItem) -> Result<(), ExtractorError> {
        let mut keys: Vec<&String> = item.attributes.keys().collect();
        keys.sort_unstable();

        for key in keys {
            let value = &item.attributes[key];
            match &value.value {
                Some(Value::StringValue(s)) => {
                    self.str_.keys().append_value(key);
                    self.str_.values().append_value(s);
                }
                Some(Value::IntValue(i)) => {
                    self.int.keys().append_value(key);
                    self.int.values().append_value(*i);
                }
                Some(Value::DoubleValue(d)) => {
                    self.double.keys().append_value(key);
                    self.double.values().append_value(*d);
                }
                Some(Value::BoolValue(b)) => {
                    self.bool_.keys().append_value(key);
                    self.bool_.values().append_value(*b);
                }
                Some(Value::BytesValue(b)) => {
                    self.bytes.keys().append_value(key);
                    self.bytes.values().append_value(b);
                }
                Some(Value::ArrayValue(_)) | Some(Value::KvlistValue(_)) => {
                    self.str_.keys().append_value(key);
                    self.str_
                        .values()
                        .append_value(any_value_to_json(value).to_string());
                }
                // An attribute whose oneof is unset carries no information.
                None => {}
            }
        }

        // Every builder must be closed on every row, including the ones that got
        // no entries; skipping one silently misaligns all later rows.
        self.str_.append(true)?;
        self.int.append(true)?;
        self.double.append(true)?;
        self.bool_.append(true)?;
        self.bytes.append(true)?;
        Ok(())
    }

    fn finish(mut self) -> [ArrayRef; 5] {
        [
            Arc::new(self.str_.finish()),
            Arc::new(self.int.finish()),
            Arc::new(self.double.finish()),
            Arc::new(self.bool_.finish()),
            Arc::new(self.bytes.finish()),
        ]
    }
}

impl Extractor for TraceItemExtractor {
    fn resource(&self) -> &'static str {
        RESOURCE
    }

    fn schema(&self) -> SchemaRef {
        SCHEMA.clone()
    }

    fn extract(&self, payloads: &[&[u8]]) -> Result<RecordBatch, ExtractorError> {
        let n = payloads.len();

        let mut organization_id = UInt64Builder::with_capacity(n);
        let mut project_id = UInt64Builder::with_capacity(n);
        let mut trace_id = StringBuilder::with_capacity(n, n * 32);
        let mut item_id = BinaryBuilder::with_capacity(n, n * 16);
        let mut item_type = StringBuilder::with_capacity(n, n * 24);
        let mut timestamp = TimestampMicrosecondBuilder::with_capacity(n);
        let mut client_sample_rate = Float64Builder::with_capacity(n);
        let mut server_sample_rate = Float64Builder::with_capacity(n);
        let mut conversation_id = StringBuilder::with_capacity(n, n * 16);
        let mut session_id = StringBuilder::with_capacity(n, n * 16);
        let mut retention_days = UInt32Builder::with_capacity(n);
        let mut received = TimestampMicrosecondBuilder::with_capacity(n);
        let mut downsampled_retention_days = UInt32Builder::with_capacity(n);
        let mut attributes = AttributeBuilders::new();

        for (index, payload) in payloads.iter().enumerate() {
            let item = TraceItem::decode(*payload)
                .map_err(|source| ExtractorError::Decode { index, source })?;

            organization_id.append_value(item.organization_id);
            project_id.append_value(item.project_id);
            trace_id.append_value(&item.trace_id);
            item_id.append_value(&item.item_id);
            item_type.append_value(item_type_name(item.item_type));
            timestamp.append_option(
                item.timestamp
                    .as_ref()
                    .map(|t| timestamp_micros(t, "timestamp"))
                    .transpose()?,
            );
            client_sample_rate.append_value(item.client_sample_rate);
            server_sample_rate.append_value(item.server_sample_rate);
            conversation_id.append_value(&item.conversation_id);
            session_id.append_value(&item.session_id);
            retention_days.append_value(item.retention_days);
            received.append_option(
                item.received
                    .as_ref()
                    .map(|t| timestamp_micros(t, "received"))
                    .transpose()?,
            );
            downsampled_retention_days.append_value(item.downsampled_retention_days);
            attributes.append_row(&item)?;
        }

        let [attr_str, attr_int, attr_double, attr_bool, attr_bytes] = attributes.finish();

        let columns: Vec<ArrayRef> = vec![
            Arc::new(organization_id.finish()),
            Arc::new(project_id.finish()),
            Arc::new(trace_id.finish()),
            Arc::new(item_id.finish()),
            Arc::new(item_type.finish()),
            Arc::new(timestamp.finish().with_timezone(TIMESTAMP_TZ)),
            Arc::new(client_sample_rate.finish()),
            Arc::new(server_sample_rate.finish()),
            Arc::new(conversation_id.finish()),
            Arc::new(session_id.finish()),
            Arc::new(retention_days.finish()),
            Arc::new(received.finish().with_timezone(TIMESTAMP_TZ)),
            Arc::new(downsampled_retention_days.finish()),
            attr_str,
            attr_int,
            attr_double,
            attr_bool,
            attr_bytes,
        ];

        Ok(RecordBatch::try_new(SCHEMA.clone(), columns)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BinaryArray, BooleanArray, Float64Array, Int64Array, MapArray, StringArray,
        TimestampMicrosecondArray, UInt32Array, UInt64Array,
    };
    use prost::Message;
    use prost_types::Timestamp;
    use sentry_protos::snuba::v1::{
        any_value::Value, AnyValue, ArrayValue, KeyValue, KeyValueList, TraceItem,
    };
    use std::collections::HashMap;

    fn extract(items: &[TraceItem]) -> RecordBatch {
        let encoded: Vec<Vec<u8>> = items.iter().map(|i| i.encode_to_vec()).collect();
        let refs: Vec<&[u8]> = encoded.iter().map(|v| v.as_slice()).collect();
        TraceItemExtractor.extract(&refs).expect("extract")
    }

    fn attr(kind: Value) -> AnyValue {
        AnyValue { value: Some(kind) }
    }

    /// Reads one row of a `Map<Utf8, T>` column as (key, value) pairs.
    fn map_row(batch: &RecordBatch, column: &str, row: usize) -> Vec<(String, String)> {
        let col = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("no column {column}"))
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("map column");
        assert!(col.is_valid(row), "map rows are never null");
        let entries = col.value(row);
        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map keys are Utf8");
        let values = entries.column(1);

        (0..entries.len())
            .map(|i| (keys.value(i).to_string(), scalar_to_string(values, i)))
            .collect()
    }

    fn scalar_to_string(values: &arrow::array::ArrayRef, i: usize) -> String {
        use arrow::datatypes::DataType;
        match values.data_type() {
            DataType::Utf8 => values
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i)
                .to_string(),
            DataType::Int64 => values
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
                .to_string(),
            DataType::Float64 => values
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(i)
                .to_string(),
            DataType::Boolean => values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(i)
                .to_string(),
            DataType::Binary => {
                let b = values.as_any().downcast_ref::<BinaryArray>().unwrap();
                String::from_utf8_lossy(b.value(i)).to_string()
            }
            other => panic!("unexpected map value type {other:?}"),
        }
    }

    fn str_col<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    fn ts_col<'a>(batch: &'a RecordBatch, name: &str) -> &'a TimestampMicrosecondArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
    }

    #[test]
    fn every_scalar_field_lands_in_its_column() {
        let item = TraceItem {
            organization_id: 42,
            project_id: 7,
            trace_id: "abc123".into(),
            item_id: vec![1, 2, 3, 4],
            item_type: 1, // TRACE_ITEM_TYPE_SPAN
            timestamp: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 123_456_000,
            }),
            client_sample_rate: 0.5,
            server_sample_rate: 0.25,
            conversation_id: "conv".into(),
            session_id: "sess".into(),
            retention_days: 90,
            received: Some(Timestamp {
                seconds: 1_700_000_001,
                nanos: 0,
            }),
            downsampled_retention_days: 30,
            ..Default::default()
        };

        let batch = extract(&[item]);
        assert_eq!(batch.num_rows(), 1);

        let u64_col = |n: &str| {
            batch
                .column_by_name(n)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0)
        };
        let u32_col = |n: &str| {
            batch
                .column_by_name(n)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(0)
        };
        let f64_col = |n: &str| {
            batch
                .column_by_name(n)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
        };

        assert_eq!(u64_col("organization_id"), 42);
        assert_eq!(u64_col("project_id"), 7);
        assert_eq!(str_col(&batch, "trace_id").value(0), "abc123");
        assert_eq!(
            batch
                .column_by_name("item_id")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0),
            &[1, 2, 3, 4]
        );
        assert_eq!(
            str_col(&batch, "item_type").value(0),
            "TRACE_ITEM_TYPE_SPAN"
        );
        assert_eq!(
            ts_col(&batch, "timestamp").value(0),
            1_700_000_000_123_456_i64
        );
        assert_eq!(f64_col("client_sample_rate"), 0.5);
        assert_eq!(f64_col("server_sample_rate"), 0.25);
        assert_eq!(str_col(&batch, "conversation_id").value(0), "conv");
        assert_eq!(str_col(&batch, "session_id").value(0), "sess");
        assert_eq!(u32_col("retention_days"), 90);
        assert_eq!(
            ts_col(&batch, "received").value(0),
            1_700_000_001_000_000_i64
        );
        assert_eq!(u32_col("downsampled_retention_days"), 30);
    }

    #[test]
    fn each_any_value_arm_lands_in_its_own_map() {
        let item = TraceItem {
            attributes: HashMap::from([
                ("s".into(), attr(Value::StringValue("hello".into()))),
                ("b".into(), attr(Value::BoolValue(true))),
                ("i".into(), attr(Value::IntValue(-9))),
                ("d".into(), attr(Value::DoubleValue(1.5))),
                ("y".into(), attr(Value::BytesValue(b"raw".to_vec()))),
            ]),
            ..Default::default()
        };

        let batch = extract(&[item]);
        assert_eq!(
            map_row(&batch, "attr_str", 0),
            vec![("s".into(), "hello".into())]
        );
        assert_eq!(
            map_row(&batch, "attr_bool", 0),
            vec![("b".into(), "true".into())]
        );
        assert_eq!(
            map_row(&batch, "attr_int", 0),
            vec![("i".into(), "-9".into())]
        );
        assert_eq!(
            map_row(&batch, "attr_double", 0),
            vec![("d".into(), "1.5".into())]
        );
        assert_eq!(
            map_row(&batch, "attr_bytes", 0),
            vec![("y".into(), "raw".into())]
        );
    }

    #[test]
    fn recursive_values_are_json_encoded_into_attr_str() {
        let item = TraceItem {
            attributes: HashMap::from([
                (
                    "arr".into(),
                    attr(Value::ArrayValue(ArrayValue {
                        values: vec![
                            attr(Value::IntValue(1)),
                            attr(Value::StringValue("two".into())),
                        ],
                    })),
                ),
                (
                    "kv".into(),
                    attr(Value::KvlistValue(KeyValueList {
                        values: vec![KeyValue {
                            key: "inner".into(),
                            value: Some(attr(Value::BoolValue(false))),
                        }],
                    })),
                ),
            ]),
            ..Default::default()
        };

        let batch = extract(&[item]);
        let row: HashMap<String, String> = map_row(&batch, "attr_str", 0).into_iter().collect();
        assert_eq!(row["arr"], r#"[1,"two"]"#);
        assert_eq!(row["kv"], r#"{"inner":false}"#);
    }

    #[test]
    fn absent_message_fields_are_null_not_epoch_zero() {
        let batch = extract(&[TraceItem::default()]);
        assert!(ts_col(&batch, "timestamp").is_null(0));
        assert!(ts_col(&batch, "received").is_null(0));
    }

    /// `conversation_id` and `session_id` are plain proto3 strings in
    /// sentry_protos 0.70: they have implicit presence, so "unset" and "empty"
    /// are indistinguishable and the column is non-nullable.
    #[test]
    fn implicit_presence_scalars_carry_defaults_and_are_never_null() {
        let batch = extract(&[TraceItem::default()]);
        for name in [
            "organization_id",
            "project_id",
            "trace_id",
            "item_id",
            "item_type",
            "client_sample_rate",
            "server_sample_rate",
            "conversation_id",
            "session_id",
            "retention_days",
            "downsampled_retention_days",
        ] {
            let col = batch.column_by_name(name).unwrap();
            assert!(!col.is_null(0), "{name} must not be null");
            assert!(
                !batch.schema().field_with_name(name).unwrap().is_nullable(),
                "{name} must be a non-nullable column"
            );
        }
        assert_eq!(str_col(&batch, "conversation_id").value(0), "");
        assert_eq!(
            str_col(&batch, "item_type").value(0),
            "TRACE_ITEM_TYPE_UNSPECIFIED"
        );
    }

    /// A newer producer sending an enum value we do not know about is a routine
    /// forward-compatible change. Crashing on it would be a self-inflicted outage.
    #[test]
    fn unknown_item_type_renders_rather_than_panicking() {
        let item = TraceItem {
            item_type: 31337,
            ..Default::default()
        };
        let batch = extract(&[item]);
        assert_eq!(
            str_col(&batch, "item_type").value(0),
            "TRACE_ITEM_TYPE_UNKNOWN_31337"
        );
    }

    /// prost decodes `map<string, AnyValue>` into a HashMap with nondeterministic
    /// iteration order. Without sorting, batches would not be reproducible.
    #[test]
    fn attribute_order_is_deterministic() {
        let forward = TraceItem {
            attributes: HashMap::from([
                ("a".into(), attr(Value::IntValue(1))),
                ("b".into(), attr(Value::IntValue(2))),
                ("c".into(), attr(Value::IntValue(3))),
            ]),
            ..Default::default()
        };
        let first = extract(&[forward.clone()]);
        let second = extract(&[forward]);
        assert_eq!(first, second);
        assert_eq!(
            map_row(&first, "attr_int", 0),
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
                ("c".into(), "3".into())
            ]
        );
    }

    /// Every map builder must be appended to on every row, including rows with no
    /// attributes of that type; skipping one silently misaligns all later rows.
    #[test]
    fn map_offsets_stay_aligned_across_mixed_rows() {
        let none = TraceItem::default();
        let one = TraceItem {
            attributes: HashMap::from([("only".into(), attr(Value::IntValue(1)))]),
            ..Default::default()
        };
        let many = TraceItem {
            attributes: HashMap::from([
                ("x".into(), attr(Value::IntValue(10))),
                ("y".into(), attr(Value::IntValue(20))),
                ("z".into(), attr(Value::StringValue("s".into()))),
            ]),
            ..Default::default()
        };

        let batch = extract(&[none, one, many]);
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(map_row(&batch, "attr_int", 0), vec![]);
        assert_eq!(
            map_row(&batch, "attr_int", 1),
            vec![("only".into(), "1".into())]
        );
        assert_eq!(
            map_row(&batch, "attr_int", 2),
            vec![("x".into(), "10".into()), ("y".into(), "20".into())]
        );
        assert_eq!(
            map_row(&batch, "attr_str", 2),
            vec![("z".into(), "s".into())]
        );
        assert_eq!(map_row(&batch, "attr_str", 0), vec![]);
    }

    #[test]
    fn truncated_payload_names_the_row() {
        let good = TraceItem {
            organization_id: 1,
            ..Default::default()
        }
        .encode_to_vec();
        let bad = vec![0xffu8, 0xff, 0xff];
        let payloads: Vec<&[u8]> = vec![good.as_slice(), bad.as_slice()];

        match TraceItemExtractor.extract(&payloads) {
            Err(ExtractorError::Decode { index, .. }) => assert_eq!(index, 1),
            other => panic!("expected a decode error naming row 1, got {other:?}"),
        }
    }

    #[test]
    fn timestamp_overflow_is_an_error_not_a_silent_wrap() {
        let item = TraceItem {
            timestamp: Some(Timestamp {
                seconds: i64::MAX,
                nanos: 0,
            }),
            ..Default::default()
        };
        let encoded = item.encode_to_vec();
        match TraceItemExtractor.extract(&[encoded.as_slice()]) {
            Err(ExtractorError::Field { field, .. }) => assert_eq!(field, "timestamp"),
            other => panic!("expected a field error, got {other:?}"),
        }
    }

    #[test]
    fn empty_batch_still_carries_the_schema() {
        let batch = TraceItemExtractor.extract(&[]).expect("empty batch");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), TraceItemExtractor.schema());
        assert_eq!(
            batch.num_columns(),
            TraceItemExtractor.schema().fields().len()
        );
    }

    #[test]
    fn produced_batch_matches_the_declared_schema() {
        let batch = extract(&[TraceItem::default()]);
        assert_eq!(batch.schema(), TraceItemExtractor.schema());
    }
}
