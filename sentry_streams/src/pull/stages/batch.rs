use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{MessageMetadata, PipelineEnvelope, Stage, StageResult};
use sentry_arroyo::types::Partition;

/// Accumulates envelopes into batches and emits when the batch reaches
/// max_batch_size. Returns Skip while accumulating, Emit when flushing.
///
/// The emitted envelope contains a Vec of the accumulated payloads.
/// Offsets are merged across the batch — highest offset per partition.
pub struct BatchAccumulatorStage {
    max_batch_size: usize,
    state: Mutex<BatchState>,
}

struct BatchState {
    payloads: Vec<KafkaPayload>,
    offsets: HashMap<Partition, u64>,
    last_metadata: Option<MessageMetadata>,
    last_raw: Option<Arc<KafkaPayload>>,
}

impl BatchState {
    fn new() -> Self {
        Self {
            payloads: Vec::new(),
            offsets: HashMap::new(),
            last_metadata: None,
            last_raw: None,
        }
    }

    /// Add a message to the batch, merging its offset.
    fn accumulate(&mut self, envelope: PipelineEnvelope<KafkaPayload>) {
        self.offsets
            .entry(envelope.metadata.partition)
            .and_modify(|o| *o = (*o).max(envelope.metadata.offset))
            .or_insert(envelope.metadata.offset);
        self.payloads.push(envelope.payload);
        self.last_metadata = Some(envelope.metadata);
        self.last_raw = Some(envelope.raw);
    }

    /// Check if the batch has reached the size threshold.
    fn is_full(&self, max_batch_size: usize) -> bool {
        self.payloads.len() >= max_batch_size
    }

    /// Drain the batch into an Envelope, clearing internal state.
    fn flush(&mut self) -> PipelineEnvelope<Vec<KafkaPayload>> {
        let payloads = std::mem::take(&mut self.payloads);
        let mut metadata = self.last_metadata.take().unwrap();
        let raw = self.last_raw.take().unwrap();

        if let Some(&max_offset) = self.offsets.get(&metadata.partition) {
            metadata.offset = max_offset;
        }
        self.offsets.clear();

        PipelineEnvelope::new(payloads, metadata, raw)
    }
}

impl BatchAccumulatorStage {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            state: Mutex::new(BatchState::new()),
        }
    }
}

impl Stage for BatchAccumulatorStage {
    type In = KafkaPayload;
    type Out = Vec<KafkaPayload>;

    async fn process(
        &self,
        envelope: PipelineEnvelope<KafkaPayload>,
    ) -> StageResult<Vec<KafkaPayload>> {
        let mut state = self.state.lock().unwrap();
        state.accumulate(envelope);

        if state.is_full(self.max_batch_size) {
            StageResult::Emit(state.flush())
        } else {
            StageResult::Skip
        }
    }

    fn name(&self) -> &'static str {
        "batch_accumulator"
    }
}
