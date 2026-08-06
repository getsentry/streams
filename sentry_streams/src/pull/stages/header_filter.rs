use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{PipelineEnvelope, RejectionReason, Stage, StageResult};

/// Filters messages by checking a Kafka header for an expected integer value.
/// Messages with a matching header pass through (Emit).
/// Messages without the header or with a non-matching value are dropped (Drop).
/// Messages with an unparseable header value are rejected (Reject → DLQ).
///
/// Header values are treated as UTF-8 ASCII decimal integers (matching the
/// existing push-based HeaderIntEqualityFilter in streams).
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

    /// `Ok(true)` — header matches expected value.
    /// `Ok(false)` — header missing, empty, or different value.
    /// `Err(())` — header present but not a valid decimal integer.
    fn check_header(&self, payload: &KafkaPayload) -> Result<bool, ()> {
        let Some(headers) = payload.headers() else {
            return Ok(false);
        };
        let Some(bytes) = headers.get(&self.header_name) else {
            return Ok(false);
        };
        if bytes.is_empty() {
            return Ok(false);
        }
        let parsed = std::str::from_utf8(bytes)
            .map_err(|_| ())?
            .parse::<i64>()
            .map_err(|_| ())?;
        Ok(parsed == self.expected_value)
    }
}

impl Stage for HeaderFilterStage {
    type In = KafkaPayload;
    type Out = KafkaPayload;

    async fn process(
        &self,
        envelope: PipelineEnvelope<KafkaPayload>,
    ) -> StageResult<KafkaPayload> {
        match self.check_header(&envelope.payload) {
            Ok(true) => StageResult::Emit(envelope),
            Ok(false) => StageResult::drop(envelope),
            Err(()) => StageResult::reject(envelope, RejectionReason::Invalid),
        }
    }

    fn name(&self) -> &'static str {
        "header_filter"
    }
}
