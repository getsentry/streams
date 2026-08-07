use sentry_arroyo::processing::stream::{PipelineEnvelope, RejectionReason, Stage, StageResult};

use crate::pull::pipeline_value::{IntoPipelineValue, PipelineValue, PipelineValueCaster};

/// Filters messages by checking a Kafka header for an expected integer value.
/// Messages with a matching header pass through (Emit).
/// Messages without the header or with a non-matching value are dropped (Drop).
/// Messages with an unparseable header value are rejected (Reject → DLQ).
///
/// Expects PipelineValue::Raw on input, produces PipelineValue::Raw on output.
pub struct HeaderFilterStage {
    header_name: String,
    expected_value: i64,
}

impl HeaderFilterStage {
    pub fn new(header_name: impl Into<String>, expected_value: i64) -> Self {
        Self {
            header_name: header_name.into(),
            expected_value,
        }
    }
}

impl Stage for HeaderFilterStage {
    type In = PipelineValue;
    type Out = PipelineValue;

    async fn process(
        &self,
        envelope: PipelineEnvelope<PipelineValue>,
    ) -> StageResult<PipelineValue> {
        let envelope = match envelope.downcast_raw() {
            Ok(e) => e,
            Err(fail) => return fail,
        };

        let payload = &envelope.payload;
        let check = match payload.headers() {
            None => Ok(false),
            Some(headers) => match headers.get(&self.header_name) {
                None => Ok(false),
                Some(bytes) if bytes.is_empty() => Ok(false),
                Some(bytes) => std::str::from_utf8(bytes)
                    .map_err(|_| ())
                    .and_then(|s| s.parse::<i64>().map_err(|_| ()))
                    .map(|v| v == self.expected_value),
            },
        };

        match check {
            Ok(true) => StageResult::Emit(envelope.into_pipeline_value()),
            Ok(false) => StageResult::Drop {
                metadata: envelope.metadata,
            },
            Err(()) => StageResult::Reject {
                metadata: envelope.metadata,
                raw: envelope.raw,
                reason: RejectionReason::Invalid,
            },
        }
    }

    fn name(&self) -> &str {
        "header_filter"
    }
}
