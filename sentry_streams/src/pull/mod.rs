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

    /// Helper to create a test envelope with no headers.
    fn make_envelope(payload: &[u8], offset: u64) -> StageResult<KafkaPayload> {
        let kafka_payload = KafkaPayload::new(None, None, Some(payload.to_vec()));
        let metadata = MessageMetadata {
            partition: Partition::new(Topic::new("test"), 0),
            offset,
            timestamp: chrono::Utc::now(),
        };
        StageResult::Emit(PipelineEnvelope::new(
            kafka_payload.clone(),
            metadata,
            Arc::new(kafka_payload),
        ))
    }

    /// Helper to create a test envelope with a header.
    fn make_envelope_with_header(
        payload: &[u8],
        offset: u64,
        header_name: &str,
        header_value: i64,
    ) -> StageResult<KafkaPayload> {
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
            kafka_payload.clone(),
            metadata,
            Arc::new(kafka_payload),
        ))
    }

    /// Helper to create a test envelope with an invalid (non-integer) header.
    fn make_envelope_with_bad_header(
        payload: &[u8],
        offset: u64,
        header_name: &str,
        header_value: &[u8],
    ) -> StageResult<KafkaPayload> {
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
            kafka_payload.clone(),
            metadata,
            Arc::new(kafka_payload),
        ))
    }

    #[tokio::test]
    async fn test_header_filter_passes_matching() {
        let committer = MockCommitter::new();
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);
        let filter = HeaderFilterStage::new("item_type", 1);
        let error_handler = LogHandler;

        // 3 messages with matching header, 2 without
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

        // All 5 offsets should be tracked (3 Emit + 2 Drop)
        let committed = committer.committed();
        assert!(!committed.is_empty(), "Expected at least one commit");
        let last = committed.last().unwrap();
        let partition = Partition::new(Topic::new("test"), 0);
        assert_eq!(last.get(&partition), Some(&5));
    }

    #[tokio::test]
    async fn test_pipeline_filter_then_batch() {
        let committer = MockCommitter::new();
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);
        let filter = HeaderFilterStage::new("item_type", 1);
        let batch = BatchAccumulatorStage::new(3);
        let error_handler = LogHandler;

        // 5 messages, all with matching header
        let messages: Vec<StageResult<KafkaPayload>> = (0..5)
            .map(|i| make_envelope_with_header(
                format!("msg-{i}").as_bytes(), i, "item_type", 1,
            ))
            .collect();

        // Track how many batches we receive and their sizes
        let batch_sizes: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
        let batch_sizes_clone = batch_sizes.clone();

        // Use a counting stage after the batch to record batch sizes
        struct CountBatchStage {
            sizes: Arc<Mutex<Vec<usize>>>,
        }
        impl Stage for CountBatchStage {
            type In = Vec<KafkaPayload>;
            type Out = Vec<KafkaPayload>;
            async fn process(
                &self,
                envelope: PipelineEnvelope<Vec<KafkaPayload>>,
            ) -> StageResult<Vec<KafkaPayload>> {
                self.sizes.lock().unwrap().push(envelope.payload.len());
                StageResult::Emit(envelope)
            }
            fn name(&self) -> &'static str { "count_batch" }
        }

        let counter = CountBatchStage { sizes: batch_sizes_clone };

        let result = stream::iter(messages)
            .apply(&filter)
            .apply(&batch)
            .apply(&counter)
            .on_reject(&error_handler)
            .commit(&mut tracker)
            .await;

        assert!(result.is_ok());

        // With batch size 3 and 5 messages: first batch has 3, remaining 2 are
        // still in the accumulator (not flushed since stream ended without
        // reaching batch size again).
        let sizes = batch_sizes.lock().unwrap();
        assert_eq!(*sizes, vec![3], "Expected one batch of 3 (remaining 2 not flushed)");

        // Offsets: batch of 3 emitted with last message's offset (2), so
        // tracker sees offset 3. The 2 remaining messages returned Skip,
        // so their offsets are not tracked.
        let committed = committer.committed();
        assert!(!committed.is_empty());
        let last = committed.last().unwrap();
        let partition = Partition::new(Topic::new("test"), 0);
        assert_eq!(last.get(&partition), Some(&3),
            "Expected offset 3 (batch last offset 2 + 1)");
    }

    #[tokio::test]
    async fn test_header_filter_rejects_invalid_header() {
        let committer = MockCommitter::new();
        let mut tracker = OffsetTracker::new(Duration::from_millis(1), &committer);
        let filter = HeaderFilterStage::new("item_type", 1);
        let error_handler = LogHandler;

        let messages = vec![
            make_envelope_with_header(b"good", 0, "item_type", 1),          // match → Emit
            make_envelope_with_bad_header(b"bad", 1, "item_type", b"not-an-int"), // invalid → Reject
            make_envelope_with_header(b"also-good", 2, "item_type", 1),     // match → Emit
        ];

        let result = stream::iter(messages)
            .apply(&filter)
            .on_reject(&error_handler)
            .commit(&mut tracker)
            .await;

        assert!(result.is_ok());

        // All 3 offsets should be tracked (2 Emit + 1 Reject)
        let committed = committer.committed();
        assert!(!committed.is_empty());
        let last = committed.last().unwrap();
        let partition = Partition::new(Topic::new("test"), 0);
        assert_eq!(last.get(&partition), Some(&3));
    }
}
