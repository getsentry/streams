//! Hand-written protobuf -> Arrow extractors, one per message type.
//!
//! Each extractor owns a hardcoded Arrow schema and knows how to turn a batch of
//! encoded payloads into a `RecordBatch`. This is the PoC trade recorded in
//! `docs/design/arrow-batch-parser.md`: Rust has no equivalent of Python's
//! reflective `ProtobufCodec`, so rather than carry a descriptor pool we write
//! the mapping by hand and accept that adding a column is a code change.
//!
//! Adding a message type is one new file plus one line in [`REGISTRY`].
//!
//! Extractors are indexed by the raw resource string from `sentry-kafka-schemas`
//! (for example `sentry_protos.snuba.v1.trace_item_pb2.TraceItem`), so several
//! topics sharing a schema share one extractor.

pub mod trace_item;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::LazyLock;

/// Why a batch could not be turned into a `RecordBatch`.
///
/// Every variant is fatal for the whole batch: offsets are collapsed to max per
/// partition on flush, so there is no `(partition, offset)` to dead-letter a
/// single row with. The step turns these into a panic; see decision 14.
#[derive(Debug)]
pub enum ExtractorError {
    /// A payload was not valid protobuf. `index` is the row's position in the
    /// batch, which is the most specific thing we can say about it.
    Decode {
        index: usize,
        source: prost::DecodeError,
    },
    /// Arrow rejected the assembled columns.
    Build(arrow::error::ArrowError),
    /// A field held a value we cannot represent, for example a timestamp that
    /// overflows microseconds.
    Field { field: &'static str, detail: String },
}

impl fmt::Display for ExtractorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExtractorError::Decode { index, source } => {
                write!(f, "row {index} is not valid protobuf: {source}")
            }
            ExtractorError::Build(e) => write!(f, "could not build the Arrow record batch: {e}"),
            ExtractorError::Field { field, detail } => {
                write!(f, "field `{field}` could not be represented: {detail}")
            }
        }
    }
}

impl std::error::Error for ExtractorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ExtractorError::Decode { source, .. } => Some(source),
            ExtractorError::Build(e) => Some(e),
            ExtractorError::Field { .. } => None,
        }
    }
}

impl From<arrow::error::ArrowError> for ExtractorError {
    fn from(e: arrow::error::ArrowError) -> Self {
        ExtractorError::Build(e)
    }
}

/// Decodes a batch of payloads of one message type into an Arrow `RecordBatch`.
pub trait Extractor: Send + Sync {
    /// The `sentry-kafka-schemas` resource string this extractor handles.
    fn resource(&self) -> &'static str;

    /// The Arrow schema of every batch this extractor produces, including for an
    /// empty batch.
    fn schema(&self) -> SchemaRef;

    /// Decode every payload and assemble one `RecordBatch`.
    ///
    /// Batch-wise by design: one Arrow builder per column fed across all rows and
    /// finished once, rather than a batch per row. That is the shape Arrow
    /// builders want, and it keeps a second extractor down to one file.
    fn extract(&self, payloads: &[&[u8]]) -> Result<RecordBatch, ExtractorError>;
}

static TRACE_ITEM: trace_item::TraceItemExtractor = trace_item::TraceItemExtractor;

/// Every extractor the runtime knows about, by resource string.
static REGISTRY: LazyLock<BTreeMap<&'static str, &'static dyn Extractor>> = LazyLock::new(|| {
    let extractors: [&'static dyn Extractor; 1] = [&TRACE_ITEM];
    extractors.into_iter().map(|e| (e.resource(), e)).collect()
});

/// The extractor for `resource`, or `None` if the message type is not supported.
pub fn get_extractor(resource: &str) -> Option<&'static dyn Extractor> {
    REGISTRY.get(resource).copied()
}

/// Every supported resource string, for error messages that tell the operator
/// what they could have used instead.
pub fn registered_resources() -> Vec<&'static str> {
    REGISTRY.keys().copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_item_is_registered_under_its_resource_string() {
        let resource = "sentry_protos.snuba.v1.trace_item_pb2.TraceItem";
        let extractor = get_extractor(resource).expect("TraceItem must be registered");
        assert_eq!(extractor.resource(), resource);
    }

    /// The registry key must match what `sentry-kafka-schemas` actually reports
    /// for the topic, otherwise lookup silently fails at startup.
    #[test]
    fn registry_key_matches_the_schema_registry() {
        let schema = sentry_kafka_schemas::get_schema("snuba-items", None).unwrap();
        assert!(
            get_extractor(schema.raw_schema()).is_some(),
            "snuba-items reports resource {:?}, which is not registered; known: {:?}",
            schema.raw_schema(),
            registered_resources()
        );
    }

    #[test]
    fn unknown_resource_has_no_extractor() {
        assert!(get_extractor("nope.NotAThing").is_none());
        assert!(!registered_resources().is_empty());
    }
}
