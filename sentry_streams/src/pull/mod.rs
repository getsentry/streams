pub mod gcs_client;
pub mod gcs_sink_handler;
pub mod pipeline_value;
pub mod stages;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::stream;
    use sentry_arroyo::backends::kafka::types::{Headers, KafkaPayload};
    use sentry_arroyo::processing::strategies::offset_tracker::{OffsetCommitter, OffsetTracker};
    use sentry_arroyo::processing::stream::{
        LogHandler, MessageMetadata, PipelineEnvelope, PipelineExt, Stage, StageResult,
    };
    use sentry_arroyo::types::{Partition, Topic};

    use super::pipeline_value::{PipelineValue, PipelineValueCaster};
    use super::stages::batch::BatchAccumulatorStage;
    use super::stages::header_filter::HeaderFilterStage;

    /// Mock committer that records what was committed.
    struct MockCommitter {
        committed: Mutex<Vec<HashMap<Partition, u64>>>,
    }

    impl MockCommitter {
        fn new() -> Self {
            Self {
                committed: Mutex::new(Vec::new()),
            }
        }

        fn committed(&self) -> Vec<HashMap<Partition, u64>> {
            self.committed.lock().unwrap().clone()
        }
    }

    impl OffsetCommitter for MockCommitter {
        fn commit_offsets(
            &self,
            positions: &HashMap<Partition, u64>,
        ) -> Result<(), Box<dyn std::error::Error + Send>> {
            self.committed.lock().unwrap().push(positions.clone());
            Ok(())
        }
    }

    /// Helper to create a PipelineValue::Raw envelope with no headers.
    fn make_envelope(payload: &[u8], offset: u64) -> StageResult<PipelineValue> {
        let kafka_payload = KafkaPayload::new(None, None, Some(payload.to_vec()));
        let metadata = MessageMetadata {
            partition: Partition::new(Topic::new("test"), 0),
            offset,
            timestamp: chrono::Utc::now(),
        };
        StageResult::Emit(PipelineEnvelope::new(
            PipelineValue::Raw(kafka_payload.clone()),
            metadata,
            Arc::new(kafka_payload),
        ))
    }

    /// Helper to create a PipelineValue::Raw envelope with a header.
    fn make_envelope_with_header(
        payload: &[u8],
        offset: u64,
        header_name: &str,
        header_value: i64,
    ) -> StageResult<PipelineValue> {
        let headers = Headers::new().insert(
            header_name,
            Some(header_value.to_string().into_bytes()),
        );
        let kafka_payload = KafkaPayload::new(None, Some(headers), Some(payload.to_vec()));
        let metadata = MessageMetadata {
            partition: Partition::new(Topic::new("test"), 0),
            offset,
            timestamp: chrono::Utc::now(),
        };
        StageResult::Emit(PipelineEnvelope::new(
            PipelineValue::Raw(kafka_payload.clone()),
            metadata,
            Arc::new(kafka_payload),
        ))
    }

    /// Helper to create a PipelineValue::Raw envelope with an invalid header.
    fn make_envelope_with_bad_header(
        payload: &[u8],
        offset: u64,
        header_name: &str,
        header_value: &[u8],
    ) -> StageResult<PipelineValue> {
        let headers = Headers::new().insert(
            header_name,
            Some(header_value.to_vec()),
        );
        let kafka_payload = KafkaPayload::new(None, Some(headers), Some(payload.to_vec()));
        let metadata = MessageMetadata {
            partition: Partition::new(Topic::new("test"), 0),
            offset,
            timestamp: chrono::Utc::now(),
        };
        StageResult::Emit(PipelineEnvelope::new(
            PipelineValue::Raw(kafka_payload.clone()),
            metadata,
            Arc::new(kafka_payload),
        ))
    }

    // ── HeaderFilterStage tests ─────────────────────────────────────

    #[tokio::test]
    async fn test_header_filter_passes_matching() {
        let committer = MockCommitter::new();
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);
        let filter = HeaderFilterStage::new("item_type", 1);
        let error_handler = LogHandler;

        let messages = vec![
            make_envelope_with_header(b"span-1", 0, "item_type", 1),   // match
            make_envelope(b"no-header", 1),                              // no header → drop
            make_envelope_with_header(b"span-2", 2, "item_type", 1),   // match
            make_envelope_with_header(b"log-1", 3, "item_type", 2),    // wrong value → drop
            make_envelope_with_header(b"span-3", 4, "item_type", 1),   // match
        ];

        let result = stream::iter(messages)
            .apply(&filter)
            .on_reject(&error_handler)
            .commit(&mut tracker)
            .await;

        assert!(result.is_ok());

        let committed = committer.committed();
        assert!(!committed.is_empty(), "Expected at least one commit");
        let last = committed.last().unwrap();
        let partition = Partition::new(Topic::new("test"), 0);
        assert_eq!(last.get(&partition), Some(&5));
    }

    #[tokio::test]
    async fn test_header_filter_rejects_invalid_header() {
        let committer = MockCommitter::new();
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);
        let filter = HeaderFilterStage::new("item_type", 1);
        let error_handler = LogHandler;

        let messages = vec![
            make_envelope_with_header(b"good", 0, "item_type", 1),
            make_envelope_with_bad_header(b"bad", 1, "item_type", b"not-an-int"),
            make_envelope_with_header(b"also-good", 2, "item_type", 1),
        ];

        let result = stream::iter(messages)
            .apply(&filter)
            .on_reject(&error_handler)
            .commit(&mut tracker)
            .await;

        assert!(result.is_ok());

        let committed = committer.committed();
        assert!(!committed.is_empty());
        let last = committed.last().unwrap();
        let partition = Partition::new(Topic::new("test"), 0);
        assert_eq!(last.get(&partition), Some(&3));
    }

    // ── BatchAccumulatorStage tests ─────────────────────────────────

    #[tokio::test]
    async fn test_pipeline_filter_then_batch() {
        let committer = MockCommitter::new();
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);
        let filter = HeaderFilterStage::new("item_type", 1);
        let batch = BatchAccumulatorStage::new(3);
        let error_handler = LogHandler;

        let messages: Vec<StageResult<PipelineValue>> = (0..5)
            .map(|i| make_envelope_with_header(
                format!("msg-{i}").as_bytes(), i, "item_type", 1,
            ))
            .collect();

        // Count batch sizes via a simple stage that downcasts the Rust batch
        let batch_sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let bs = batch_sizes.clone();

        struct CountBatchStage {
            sizes: Arc<Mutex<Vec<usize>>>,
        }
        impl Stage for CountBatchStage {
            type In = PipelineValue;
            type Out = PipelineValue;
            async fn process(
                &self,
                envelope: PipelineEnvelope<PipelineValue>,
            ) -> StageResult<PipelineValue> {
                let typed = match envelope.downcast_rust::<Vec<KafkaPayload>>() {
                    Ok(t) => t,
                    Err(fail) => return fail,
                };
                self.sizes.lock().unwrap().push(typed.payload.len());
                StageResult::Emit(PipelineEnvelope::new(
                    PipelineValue::Rust(Box::new(typed.payload)),
                    typed.metadata,
                    typed.raw,
                ))
            }
            fn name(&self) -> &'static str { "count_batch" }
        }

        let counter = CountBatchStage { sizes: bs };

        let result = stream::iter(messages)
            .apply(&filter)
            .apply(&batch)
            .apply(&counter)
            .on_reject(&error_handler)
            .commit(&mut tracker)
            .await;

        assert!(result.is_ok());

        let sizes = batch_sizes.lock().unwrap();
        assert_eq!(*sizes, vec![3], "Expected one batch of 3 (remaining 2 not flushed)");

        let committed = committer.committed();
        assert!(!committed.is_empty());
        let last = committed.last().unwrap();
        let partition = Partition::new(Topic::new("test"), 0);
        assert_eq!(last.get(&partition), Some(&3),
            "Expected offset 3 (batch last offset 2 + 1)");
    }

    // ── Full pipeline integration test ──────────────────────────────

    #[tokio::test]
    async fn test_full_pipeline_filter_batch() {
        let committer = MockCommitter::new();
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);
        let filter = HeaderFilterStage::new("item_type", 1);
        let batch = BatchAccumulatorStage::new(2);
        let error_handler = LogHandler;

        // Track batches and their content
        let batches: Arc<Mutex<Vec<Vec<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));
        let batches_clone = batches.clone();

        struct CollectBatchStage {
            batches: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
        }
        impl Stage for CollectBatchStage {
            type In = PipelineValue;
            type Out = PipelineValue;
            async fn process(
                &self,
                envelope: PipelineEnvelope<PipelineValue>,
            ) -> StageResult<PipelineValue> {
                let typed = match envelope.downcast_rust::<Vec<KafkaPayload>>() {
                    Ok(t) => t,
                    Err(fail) => return fail,
                };
                let contents: Vec<Vec<u8>> = typed.payload.iter()
                    .map(|kp| kp.payload().map(|v| v.to_vec()).unwrap_or_default())
                    .collect();
                self.batches.lock().unwrap().push(contents);
                StageResult::Emit(PipelineEnvelope::new(
                    PipelineValue::Rust(Box::new(typed.payload)),
                    typed.metadata,
                    typed.raw,
                ))
            }
            fn name(&self) -> &'static str { "collect_batch" }
        }

        let collector = CollectBatchStage { batches: batches_clone };

        // 4 messages with matching header → 2 batches of 2
        let messages: Vec<StageResult<PipelineValue>> = (0..4)
            .map(|i| make_envelope_with_header(
                format!("span-{i}").as_bytes(), i, "item_type", 1,
            ))
            .collect();

        let result = stream::iter(messages)
            .apply(&filter)
            .apply(&batch)
            .apply(&collector)
            .on_reject(&error_handler)
            .commit(&mut tracker)
            .await;

        assert!(result.is_ok());

        let collected = batches.lock().unwrap();
        assert_eq!(collected.len(), 2, "Expected 2 batches of 2");
        assert_eq!(collected[0], vec![b"span-0".to_vec(), b"span-1".to_vec()]);
        assert_eq!(collected[1], vec![b"span-2".to_vec(), b"span-3".to_vec()]);
    }
}
