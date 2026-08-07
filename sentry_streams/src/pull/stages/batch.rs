use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{MessageMetadata, PipelineEnvelope, Stage, StageResult};
use sentry_arroyo::types::Partition;

use crate::pull::pipeline_value::{PipelineValue, PipelineValueCaster};

/// Accumulates envelopes into batches and emits when the batch reaches
/// max_batch_size. Returns Skip while accumulating, Emit when flushing.
///
/// Expects PipelineValue::Raw on input. Emits PipelineValue::Rust(Vec<KafkaPayload>)
/// on flush. Offsets are merged across the batch — highest offset per partition.
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

    fn accumulate(
        &mut self,
        payload: KafkaPayload,
        metadata: MessageMetadata,
        raw: Arc<KafkaPayload>,
    ) {
        self.offsets
            .entry(metadata.partition)
            .and_modify(|o| *o = (*o).max(metadata.offset))
            .or_insert(metadata.offset);
        self.payloads.push(payload);
        self.last_metadata = Some(metadata);
        self.last_raw = Some(raw);
    }

    fn is_full(&self, max_batch_size: usize) -> bool {
        self.payloads.len() >= max_batch_size
    }

    fn flush(&mut self) -> PipelineEnvelope<PipelineValue> {
        let payloads = std::mem::take(&mut self.payloads);
        let mut metadata = self.last_metadata.take().unwrap();
        let raw = self.last_raw.take().unwrap();

        if let Some(&max_offset) = self.offsets.get(&metadata.partition) {
            metadata.offset = max_offset;
        }
        self.offsets.clear();

        PipelineEnvelope::new(PipelineValue::Rust(Box::new(payloads)), metadata, raw)
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

        let mut state = self.state.lock().unwrap();
        state.accumulate(envelope.payload, envelope.metadata, envelope.raw);

        if state.is_full(self.max_batch_size) {
            StageResult::Emit(state.flush())
        } else {
            StageResult::Skip
        }
    }

    fn name(&self) -> &str {
        "batch_accumulator"
    }
}
