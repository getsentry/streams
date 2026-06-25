//! Batches streaming messages on a route. [`Batch`] stores [`PyStreamingMessage`] values; `PyAnyMessage`
//! and `RawMessage` may appear in the same window. On flush, output is a single `PyAnyMessage` whose
//! `payload` is a Python `list` with one item per element (each item is the row’s Python payload for
//! `PyAnyMessage`, or `bytes` for `RawMessage`). The batched `schema` is taken from the first
//! element. Watermark handling and backpressure are unchanged.
//!
//! The GIL is taken only to build the list on flush (after [`Message::into_payload`] in submit).
use crate::messages::{into_pyany, PyAnyMessage, PyStreamingMessage, RoutedValuePayload};
use crate::pipeline_stats::get_stats;
use crate::routes::{Route, RoutedValue};
use crate::time_helpers::current_epoch;
use crate::utils::traced_with_gil;
use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, MessageRejected, ProcessingStrategy, StrategyError,
    SubmitError,
};
use sentry_arroyo::types::{Message, Partition};
use sentry_arroyo::utils::timing::Deadline;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const METRIC_BATCH_SIZE: &str = "streams.pipeline.batch.size";
const METRIC_BATCH_TIME_MS: &str = "streams.pipeline.batch.time_ms";
const METRIC_BATCH_SUBMIT_DURATION_MS: &str = "streams.pipeline.batch.submit_duration_ms";
const METRIC_BATCH_SUBMIT_REJECTED: &str = "streams.pipeline.batch.submit_rejected";

/// How often the debounced "MessageRejected" log line and counter metric may be
/// emitted. Submits are retried in a tight loop under backpressure, so we
/// aggregate rejections instead of emitting one per attempt.
const REJECTED_DEBOUNCE_INTERVAL: Duration = Duration::from_secs(3);

fn first_element_schema(py: Python<'_>, first: &PyStreamingMessage) -> Option<String> {
    match first {
        PyStreamingMessage::PyAnyMessage { content } => content.bind(py).borrow().schema.clone(),
        PyStreamingMessage::RawMessage { content } => content.bind(py).borrow().schema.clone(),
    }
}

/// Returns the Python object to be added to the produced batch. This is
/// the Python object that contains the payload of the message.
fn list_item_for_streaming_message(
    py: Python<'_>,
    pysm: &PyStreamingMessage,
) -> PyResult<Py<PyAny>> {
    match pysm {
        PyStreamingMessage::PyAnyMessage { content } => {
            Ok(content.bind(py).borrow().payload.clone_ref(py))
        }
        PyStreamingMessage::RawMessage { content } => {
            // RawMessages payload is turned into a Python bytes object as
            // we do not have a native Rust batch message.
            let p = &content.bind(py).borrow().payload;
            Ok(PyBytes::new(py, p).into_any().unbind())
        }
    }
}

/// Count- and/or time-based window of streaming elements for one route. On flush, output is
/// always a batched `PyAnyMessage` with a list payload.
pub(crate) struct Batch {
    route: Route,
    max_batch_size: Option<usize>,
    /// Set when the window is time-bounded; elapsed means flush by time.
    batch_deadline: Option<Deadline>,
    /// Wall time when the first element opened this batch window.
    created_at: Instant,
    elements: Vec<PyStreamingMessage>,
    batch_offsets: BTreeMap<Partition, u64>,
}

impl Batch {
    /// First element in a window. `committable` and `first` are from the same [`RoutedValue`]
    /// (see [`BatchStep::submit`]). Later elements may use either `PyAnyMessage` or `RawMessage`.
    pub fn from_initial(
        route: Route,
        max_batch_size: Option<usize>,
        max_batch_time: Option<Duration>,
        // Keeps track of the highest offset for each partition. This represent the committable
        // we will return when the batch is flushed.
        committable: BTreeMap<Partition, u64>,
        first: PyStreamingMessage,
    ) -> Self {
        let mut batch_offsets: BTreeMap<Partition, u64> = BTreeMap::new();
        for (p, o) in committable {
            batch_offsets
                .entry(p)
                .and_modify(|e| *e = (*e).max(o))
                .or_insert(o);
        }
        let batch_deadline = max_batch_time.map(Deadline::new);
        Self {
            route,
            max_batch_size,
            batch_deadline,
            created_at: Instant::now(),
            elements: vec![first],
            batch_offsets,
        }
    }

    pub fn append(&mut self, committable: BTreeMap<Partition, u64>, pysm: PyStreamingMessage) {
        for (p, o) in committable {
            self.batch_offsets
                .entry(p)
                .and_modify(|e| *e = (*e).max(o))
                .or_insert(o);
        }
        self.elements.push(pysm);
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn should_flush(&self) -> bool {
        if self.is_empty() {
            return false;
        }
        if self.max_batch_size.is_some_and(|m| self.len() >= m) {
            return true;
        }
        if self
            .batch_deadline
            .as_ref()
            .is_some_and(|d| d.has_elapsed())
        {
            return true;
        }
        false
    }

    pub fn current_offsets_snapshot(&self) -> BTreeMap<Partition, u64> {
        self.batch_offsets.clone()
    }

    /// Minimum of per-row logical timestamps (epoch seconds, sub-second precision) in this batch;
    /// used as `last_message_time` on synthetic watermarks after flush.
    pub fn oldest_batch_row_timestamp(&self) -> Option<f64> {
        if self.elements.is_empty() {
            return None;
        }
        traced_with_gil!(|py| {
            let mut min_t: Option<f64> = None;
            for el in &self.elements {
                let t = match el {
                    PyStreamingMessage::PyAnyMessage { content } => {
                        content.bind(py).borrow().timestamp
                    }
                    PyStreamingMessage::RawMessage { content } => {
                        content.bind(py).borrow().timestamp
                    }
                };
                min_t = Some(min_t.map_or(t, |m| m.min(t)));
            }
            min_t
        })
    }

    pub fn flush(&self) -> Result<Message<RoutedValue>, StrategyError> {
        let route = self.route.clone();
        let committable = self.batch_offsets.clone();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let content = traced_with_gil!(|py| -> PyResult<Py<PyAnyMessage>> {
            let first_schema = first_element_schema(py, &self.elements[0]);
            let py_items: Result<Vec<Py<PyAny>>, _> = self
                .elements
                .iter()
                .map(|el| list_item_for_streaming_message(py, el))
                .collect();
            let py_items = py_items.map_err(|e: PyErr| e)?;
            let list = PyList::new(py, &py_items)?.unbind();
            let inner = PyAnyMessage {
                payload: list.into_any(),
                headers: vec![],
                timestamp: ts,
                schema: first_schema,
            };
            into_pyany(py, inner)
        })
        .map_err(|e| StrategyError::Other(Box::new(e)))?;

        let py_streaming = PyStreamingMessage::PyAnyMessage { content };
        let rv = RoutedValue {
            route,
            payload: RoutedValuePayload::PyStreamingMessage(py_streaming),
        };
        Ok(Message::new_any_message(rv, committable))
    }
}

pub struct BatchStep {
    next_step: Box<dyn ProcessingStrategy<RoutedValue>>,

    route: Route,
    step_name: String,
    max_batch_size: Option<usize>,
    max_batch_time: Option<Duration>,
    /// `None` until the first streaming message in a window.
    batch: Option<Batch>,
    /// Watermarks received while the current batch window is open; on successful batch send they
    /// are appended to [`Self::outbound`].
    ///
    /// We need to hold on the watermarks we receive until the batch is flushed otherwise we
    /// would indicate that messages have to be committed before they are sent through.
    watermark_buffer: Vec<Message<RoutedValue>>,
    /// This is the queue of messages that are ready to be pushed through to the next step.
    /// We have to keep it as part of the struct state because we are not guaranteed to be
    /// able to push it through in one call to `poll`.  We may receive MessageRejected errors
    /// on any of the messages we want to flush.
    /// In those case we hold the pending messages and try again on the next call to `poll`.
    outbound: VecDeque<Message<RoutedValue>>,
    /// When true, a batched `Message` is waiting in [`Self::outbound`] and must be delivered
    /// before this step accepts new messages.
    /// This helps us capping the size of the pending queue during prolonged backpressure periods.
    pending_batch: bool,
    commit_request_carried_over: Option<CommitRequest>,
    step_labels: Vec<(String, String)>,
    /// Rejections accumulated since the last debounced emission.
    rejected_count: u64,
    /// Wall-clock time of the first rejection in the current (un-emitted) window.
    rejected_first_at: Option<SystemTime>,
    /// Monotonic time of the last debounced emission.
    rejected_last_emitted: Option<Instant>,
}

impl BatchStep {
    pub fn new(
        route: Route,
        max_batch_size: Option<usize>,
        max_batch_time: Option<Duration>,
        step_name: String,
        next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    ) -> Self {
        let step_labels = vec![("step".to_string(), step_name.clone())];
        Self {
            next_step,
            route,
            step_name,
            max_batch_size,
            max_batch_time,
            batch: None,
            watermark_buffer: Vec::new(),
            outbound: VecDeque::new(),
            pending_batch: false,
            commit_request_carried_over: None,
            step_labels,
            rejected_count: 0,
            rejected_first_at: None,
            rejected_last_emitted: None,
        }
    }

    /// Records a `MessageRejected` outcome during outbound draining. To avoid
    /// flooding the logs and metrics in the tight backpressure retry loop, the
    /// log line and counter metric are emitted at most once per
    /// [`REJECTED_DEBOUNCE_INTERVAL`], carrying the number of occurrences since
    /// the last emission and the timestamp of the first one.
    fn record_rejected_submit(&mut self, submit_duration_ms: f64, outbound_len: usize) {
        let now = Instant::now();
        self.rejected_count += 1;
        self.rejected_first_at.get_or_insert_with(SystemTime::now);

        let due = self.rejected_last_emitted.map_or(true, |t| {
            now.duration_since(t) >= REJECTED_DEBOUNCE_INTERVAL
        });
        if !due {
            return;
        }

        let first_at = self.rejected_first_at.map(|t| DateTime::<Utc>::from(t));
        log::debug!(
            "BatchStep drain submit. step: {:?}, duration_ms: {:?}, outbound_len: {:?}, outcome: {:?}, occurrences: {:?}, first_rejected_at: {:?}",
            self.step_name,
            submit_duration_ms,
            outbound_len,
            "message_rejected",
            self.rejected_count,
            first_at,
        );
        metrics::counter!(METRIC_BATCH_SUBMIT_REJECTED, &self.step_labels)
            .increment(self.rejected_count);

        self.rejected_count = 0;
        self.rejected_first_at = None;
        self.rejected_last_emitted = Some(now);
    }

    /// Tries to drain the queue containing the pending messages.
    /// At the first MessageRejected it stops and returns the error leaving the queue
    /// intact for the following attempt.
    ///
    /// It calls `poll` on the next step to guarantee it has a chance to process the
    /// on going work.
    fn drain_outbound(&mut self) -> Result<(), StrategyError> {
        while let Some(msg) = self.outbound.pop_front() {
            let outbound_len = self.outbound.len();
            let c = self.next_step.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), c);
            let submit_start = Instant::now();
            let submit_result = self.next_step.submit(msg);
            let submit_duration_ms = submit_start.elapsed().as_secs_f64() * 1000.0;
            metrics::gauge!(METRIC_BATCH_SUBMIT_DURATION_MS, &self.step_labels)
                .set(submit_duration_ms);
            match &submit_result {
                // MessageRejected happens on every retry of a stalled submit, i.e. in a tight
                // loop, so it is debounced separately rather than logged here.
                Err(SubmitError::MessageRejected(_)) => {
                    self.record_rejected_submit(submit_duration_ms, outbound_len)
                }
                other => {
                    let submit_outcome = match other {
                        Ok(()) => "ok",
                        Err(SubmitError::InvalidMessage(_)) => "invalid_message",
                        Err(SubmitError::MessageRejected(_)) => unreachable!(),
                    };
                    log::debug!(
                        "BatchStep drain submit. step: {:?}, duration_ms: {:?}, outbound_len: {:?}, outcome: {:?}",
                        self.step_name,
                        submit_duration_ms,
                        outbound_len,
                        submit_outcome
                    );
                }
            }
            match submit_result {
                Ok(()) => {
                    if self.pending_batch {
                        self.pending_batch = false;
                    }
                }
                Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                    self.outbound.push_front(message);
                    break;
                }
                Err(SubmitError::InvalidMessage(e)) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn try_emit_batch(&mut self, force: bool) -> Result<(), StrategyError> {
        if self.pending_batch {
            return Ok(());
        }
        if self.batch.as_ref().map_or(true, |b| b.is_empty()) {
            return Ok(());
        }
        if !force && !self.batch.as_ref().map_or(true, |b| b.should_flush()) {
            return Ok(());
        }

        let b = self
            .batch
            .as_ref()
            .ok_or_else(|| StrategyError::Other("BatchStep: emit without active batch".into()))?;

        // We create a synthetic watermark to avoid waiting for the next batch to complete before
        // allowing the consumer to commit.
        let committable_for_synthetic = b.current_offsets_snapshot();
        let batch_last_message_time = b.oldest_batch_row_timestamp();
        let batch_elements = b.len() as f64;
        let batch_open_ms = b.created_at.elapsed().as_millis() as f64;
        let flush_start = Instant::now();
        let batch_msg = b.flush()?;
        get_stats().step_timing(&self.step_name, flush_start.elapsed().as_secs_f64());
        metrics::histogram!(METRIC_BATCH_SIZE, &self.step_labels).record(batch_elements);
        metrics::histogram!(METRIC_BATCH_TIME_MS, &self.step_labels).record(batch_open_ms);
        log::info!(
            "Batch flushed. step: {:?}, batch_elements: {:?}, batch_open_ms: {:?} created_at: {:?}",
            self.step_name,
            batch_elements,
            batch_open_ms,
            b.created_at
        );
        self.batch = None;
        let wm_after_batch: Vec<_> = std::mem::take(&mut self.watermark_buffer);

        self.outbound.push_back(batch_msg);
        self.pending_batch = true;
        self.enqueue_watermark_tail(
            wm_after_batch,
            committable_for_synthetic,
            batch_last_message_time,
        );
        Ok(())
    }

    fn enqueue_watermark_tail(
        &mut self,
        wm_after_batch: Vec<Message<RoutedValue>>,
        committable: BTreeMap<Partition, u64>,
        last_message_time: Option<f64>,
    ) {
        for m in wm_after_batch {
            self.outbound.push_back(m);
        }
        let ts = current_epoch();
        let wmk = Message::new_any_message(
            RoutedValue {
                route: self.route.clone(),
                payload: RoutedValuePayload::make_watermark_payload(
                    committable.clone(),
                    ts,
                    last_message_time,
                ),
            },
            committable,
        );
        self.outbound.push_back(wmk);
    }
}

#[cfg(test)]
impl BatchStep {
    /// Simulates a batched `Message` stuck behind downstream backpressure: same as a reject on
    /// the batch flush, but with no prior [`try_emit_batch`].
    pub(crate) fn set_stalled_outbound_for_test(&mut self, message: Option<Message<RoutedValue>>) {
        if let Some(m) = message {
            self.outbound.push_front(m);
            self.pending_batch = true;
        } else {
            self.pending_batch = false;
        }
    }
}

pub fn build_batch_step(
    route: &Route,
    max_batch_size: Option<usize>,
    max_batch_time: Option<Duration>,
    step_name: String,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    Box::new(BatchStep::new(
        route.clone(),
        max_batch_size,
        max_batch_time,
        step_name,
        next,
    ))
}

impl ProcessingStrategy<RoutedValue> for BatchStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.try_emit_batch(false)?;
        self.drain_outbound()?;
        let c = self.next_step.poll()?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            c,
        ))
    }

    fn submit(&mut self, message: Message<RoutedValue>) -> Result<(), SubmitError<RoutedValue>> {
        if self.route != message.payload().route {
            return self.next_step.submit(message);
        }

        if self.pending_batch {
            return Err(SubmitError::MessageRejected(MessageRejected { message }));
        }

        let committable: BTreeMap<Partition, u64> = message.committable().collect();
        let rv = message.into_payload();
        match rv.payload {
            RoutedValuePayload::WatermarkMessage(wm) => {
                if self.batch.as_ref().map_or(true, |b| b.is_empty()) {
                    return self.next_step.submit(Message::new_any_message(
                        RoutedValue {
                            route: rv.route,
                            payload: RoutedValuePayload::WatermarkMessage(wm),
                        },
                        committable,
                    ));
                }
                self.watermark_buffer.push(Message::new_any_message(
                    RoutedValue {
                        route: rv.route,
                        payload: RoutedValuePayload::WatermarkMessage(wm),
                    },
                    committable,
                ));
                Ok(())
            }
            RoutedValuePayload::PyStreamingMessage(pysm) => {
                if self.batch.is_none() {
                    self.batch = Some(Batch::from_initial(
                        self.route.clone(),
                        self.max_batch_size,
                        self.max_batch_time,
                        committable,
                        pysm,
                    ));
                } else {
                    self.batch
                        .as_mut()
                        .expect("open batch")
                        .append(committable, pysm);
                }
                get_stats().step_exec(&self.step_name);
                Ok(())
            }
        }
    }

    fn terminate(&mut self) {
        self.next_step.terminate();
    }

    fn join(&mut self, timeout: Option<Duration>) -> Result<Option<CommitRequest>, StrategyError> {
        let deadline = timeout.map(Deadline::new);
        loop {
            if deadline.as_ref().is_some_and(|d| d.has_elapsed()) {
                break;
            }
            self.try_emit_batch(true)?;
            self.drain_outbound()?;
            if !self.pending_batch
                && self.batch.as_ref().map_or(true, |b| b.is_empty())
                && self.outbound.is_empty()
                && self.watermark_buffer.is_empty()
            {
                break;
            }
        }
        let remaining = self.next_step.join(deadline.map(|d| d.remaining()))?;
        Ok(merge_commit_request(
            self.commit_request_carried_over.take(),
            remaining,
        ))
    }
}

#[cfg(test)]
mod tests {
    mod batch {
        //! [`Batch`] in isolation: elements, committable, `should_flush`, list build (GIL).

        use crate::batch_step::Batch;
        use crate::messages::{PyStreamingMessage, RoutedValuePayload};
        use crate::routes::{Route, RoutedValue};
        use crate::testutils::{build_raw_routed_value, build_routed_value};
        use crate::utils::traced_with_gil;
        use chrono::Utc;
        use pyo3::prelude::*;
        use pyo3::types::{PyAnyMethods, PyBytes, PyList};
        use pyo3::IntoPyObject;
        use sentry_arroyo::types::{Message, Partition, Topic};
        use std::collections::BTreeMap;

        fn route() -> Route {
            Route::new("s".into(), vec!["w".into()])
        }

        /// Same decomposition as [`BatchStep::submit`]: committable map + owned streaming row.
        fn committable_and_streaming(
            message: Message<RoutedValue>,
        ) -> (BTreeMap<Partition, u64>, PyStreamingMessage) {
            let c = message.committable().collect();
            let rv = message.into_payload();
            let RoutedValuePayload::PyStreamingMessage(s) = rv.payload else {
                panic!("test expects PyStreamingMessage");
            };
            (c, s)
        }

        #[test]
        fn flush_makes_list_payload() {
            traced_with_gil!(|py| {
                let r = route();
                let p1 = 1i32.into_pyobject(py).unwrap().into_any().unbind();
                let p2 = 2i32.into_pyobject(py).unwrap().into_any().unbind();
                let part = Partition::new(Topic::new("t"), 0);
                let m1 = Message::new_any_message(
                    build_routed_value(py, p1, "s", vec!["w".into()]),
                    BTreeMap::from([(part, 1u64)]),
                );
                let m2 = Message::new_any_message(
                    build_routed_value(py, p2, "s", vec!["w".into()]),
                    BTreeMap::from([(part, 2u64)]),
                );
                let (c1, el1) = committable_and_streaming(m1);
                let mut b = Batch::from_initial(r.clone(), Some(2), None, c1, el1);
                let (c2, el2) = committable_and_streaming(m2);
                b.append(c2, el2);
                let msg = b.flush().expect("build");
                assert!(
                    msg.committable().any(|(p, o)| p == part && o == 2),
                    "committable should include merged batch offsets"
                );
                let RoutedValuePayload::PyStreamingMessage(pysm) = &msg.payload().payload else {
                    panic!("expected PyStreamingMessage");
                };
                let PyStreamingMessage::PyAnyMessage { content } = pysm else {
                    panic!("expected PyAny");
                };
                let pl = content.bind(py).getattr("payload").unwrap();
                let list = pl.cast::<PyList>().unwrap();
                assert_eq!(list.len(), 2);
                assert_eq!(list.get_item(0).unwrap().extract::<i32>().unwrap(), 1);
                assert_eq!(list.get_item(1).unwrap().extract::<i32>().unwrap(), 2);
            });
        }

        #[test]
        fn flush_makes_list_of_bytes() {
            traced_with_gil!(|py| {
                let r = route();
                let part = Partition::new(Topic::new("t"), 0);
                let m1 = Message::new_any_message(
                    build_raw_routed_value(py, vec![1, 2], "s", vec!["w".into()]),
                    BTreeMap::from([(part, 1u64)]),
                );
                let m2 = Message::new_any_message(
                    build_raw_routed_value(py, vec![3], "s", vec!["w".into()]),
                    BTreeMap::from([(part, 2u64)]),
                );
                let (c1, el1) = committable_and_streaming(m1);
                let mut b = Batch::from_initial(r, Some(2), None, c1, el1);
                let (c2, el2) = committable_and_streaming(m2);
                b.append(c2, el2);
                let msg = b.flush().expect("build");
                let RoutedValuePayload::PyStreamingMessage(pysm) = &msg.payload().payload else {
                    panic!("expected PyStreamingMessage");
                };
                let PyStreamingMessage::PyAnyMessage { content } = pysm else {
                    panic!("batched output is always PyAnyMessage with list");
                };
                let pl = content.bind(py).getattr("payload").unwrap();
                let list = pl.cast::<PyList>().unwrap();
                assert_eq!(list.len(), 2);
                let b0: Vec<u8> = list
                    .get_item(0)
                    .unwrap()
                    .cast::<PyBytes>()
                    .unwrap()
                    .as_bytes()
                    .to_vec();
                let b1: Vec<u8> = list
                    .get_item(1)
                    .unwrap()
                    .cast::<PyBytes>()
                    .unwrap()
                    .as_bytes()
                    .to_vec();
                assert_eq!(b0, vec![1, 2]);
                assert_eq!(b1, vec![3]);
            });
        }

        #[test]
        fn append_merges_max_per_partition() {
            traced_with_gil!(|py| {
                let r = route();
                let p0 = 0i32.into_pyobject(py).unwrap().into_any().unbind();
                let p_for_msg = 0i32.into_pyobject(py).unwrap().into_any().unbind();
                let part = Partition::new(Topic::new("t"), 0);
                let m1 = Message::new_any_message(
                    build_routed_value(py, p0, "s", vec!["w".into()]),
                    BTreeMap::from([(part, 1u64)]),
                );
                let m2 = Message::new_broker_message(
                    build_routed_value(py, p_for_msg, "s", vec!["w".into()]),
                    part,
                    9,
                    Utc::now(),
                );
                let (c1, el1) = committable_and_streaming(m1);
                let mut b = Batch::from_initial(r, None, None, c1, el1);
                let (c2, el2) = committable_and_streaming(m2);
                b.append(c2, el2);
                let snap = b.current_offsets_snapshot();
                assert_eq!(snap.get(&part).copied(), Some(10u64));
            });
        }

        #[test]
        fn should_flush_when_max_batch_size_reached() {
            traced_with_gil!(|py| {
                let r = route();
                let p1 = 1i32.into_pyobject(py).unwrap().into_any().unbind();
                let p2 = 2i32.into_pyobject(py).unwrap().into_any().unbind();
                let part = Partition::new(Topic::new("t"), 0);
                let m1 = Message::new_any_message(
                    build_routed_value(py, p1, "s", vec!["w".into()]),
                    BTreeMap::from([(part, 0u64)]),
                );
                let m2 = Message::new_any_message(
                    build_routed_value(py, p2, "s", vec!["w".into()]),
                    BTreeMap::from([(part, 1u64)]),
                );
                let (c1, el1) = committable_and_streaming(m1);
                let mut b = Batch::from_initial(r, Some(2), None, c1, el1);
                assert!(!b.should_flush(), "one element, limit 2");
                let (c2, el2) = committable_and_streaming(m2);
                b.append(c2, el2);
                assert!(b.should_flush(), "two elements, limit 2");
            });
        }
    }

    mod step {
        //! [`BatchStep`] as [`ProcessingStrategy`]: routing, mixed streaming rows, backpressure, watermarks.

        use super::super::{BatchStep, Message};
        use crate::fake_strategy::FakeStrategy;
        use crate::testutils::{
            build_raw_routed_value, build_routed_value, build_routed_value_with_timestamp,
        };
        use crate::utils::traced_with_gil;
        use chrono::Utc;
        use pyo3::prelude::*;
        use pyo3::types::{PyBytes, PyList};
        use pyo3::IntoPyObject;
        use sentry_arroyo::processing::strategies::{
            MessageRejected, ProcessingStrategy, SubmitError,
        };
        use sentry_arroyo::types::{Partition, Topic};
        use std::collections::BTreeMap;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        use crate::messages::RoutedValuePayload;
        use crate::routes::Route;

        fn batch_step_with_fake(
            route: Route,
            max_n: Option<usize>,
            max_t: Option<Duration>,
        ) -> (
            BatchStep,
            Arc<Mutex<Vec<Py<PyAny>>>>,
            Arc<Mutex<Vec<crate::messages::Watermark>>>,
        ) {
            let sub = Arc::new(Mutex::new(Vec::new()));
            let wms = Arc::new(Mutex::new(Vec::new()));
            let next = FakeStrategy::new(sub.clone(), wms.clone(), false);
            let step = BatchStep::new(
                route,
                max_n,
                max_t,
                "test_batch".to_string(),
                Box::new(next),
            );
            (step, sub, wms)
        }

        #[test]
        fn forwards_mismatched_route_to_next_strategy() {
            let step_route = Route::new("a".into(), vec![]);
            let (mut step, captured, _wms) = batch_step_with_fake(step_route, None, None);
            traced_with_gil!(|py| {
                let p = 1i32.into_pyobject(py).unwrap().into_any().unbind();
                let rv = crate::testutils::build_routed_value(py, p, "b", vec![]);
                let m = Message::new_any_message(rv, BTreeMap::new());
                step.submit(m).expect("forward");
            });
            assert_eq!(captured.lock().unwrap().len(), 1);
        }

        #[test]
        fn flushes_one_message_to_downstream_when_batch_full() {
            let route = Route::new("s".into(), vec!["w".into()]);
            let (mut step, out, _wms) = batch_step_with_fake(route, Some(2), None);
            traced_with_gil!(|py| {
                let p1 = 1i32.into_pyobject(py).unwrap().into_any().unbind();
                let p2 = 2i32.into_pyobject(py).unwrap().into_any().unbind();
                let m1 = Message::new_any_message(
                    build_routed_value(py, p1, "s", vec!["w".into()]),
                    BTreeMap::new(),
                );
                let m2 = Message::new_any_message(
                    build_routed_value(py, p2, "s", vec!["w".into()]),
                    BTreeMap::new(),
                );
                step.submit(m1).unwrap();
                step.submit(m2).unwrap();
                step.poll().unwrap();
            });
            let n = out.lock().unwrap().len();
            assert_eq!(
                n, 1,
                "downstream should receive one batched message, not list internals"
            );
        }

        #[test]
        fn submit_accepts_raw_broker_message_after_pyany() {
            let route = Route::new("s".into(), vec!["w".into()]);
            let (mut step, _out, _wms) = batch_step_with_fake(route, Some(10), None);
            let part = Partition::new(Topic::new("topic"), 0);
            traced_with_gil!(|py| {
                let p0 = 0i32.into_pyobject(py).unwrap().into_any().unbind();
                let py_m = Message::new_broker_message(
                    build_routed_value(py, p0, "s", vec!["w".into()]),
                    part,
                    1,
                    Utc::now(),
                );
                let raw_m = Message::new_broker_message(
                    build_raw_routed_value(py, vec![1, 2, 3], "s", vec!["w".into()]),
                    part,
                    2,
                    Utc::now(),
                );
                step.submit(py_m).unwrap();
                step.submit(raw_m)
                    .expect("mixed PyAny and Raw in one batch");
            });
        }

        #[test]
        fn submit_mixed_streaming_raw_after_streaming_any_flushes_mixed_list() {
            let route = Route::new("s".into(), vec!["w".into()]);
            let (mut step, out, _wms) = batch_step_with_fake(route, Some(2), None);
            let part = Partition::new(Topic::new("topic"), 0);
            traced_with_gil!(|py| {
                let p0 = 0i32.into_pyobject(py).unwrap().into_any().unbind();
                let any_m = Message::new_any_message(
                    build_routed_value(py, p0, "s", vec!["w".into()]),
                    BTreeMap::from([(part, 1u64)]),
                );
                let raw_m = Message::new_any_message(
                    build_raw_routed_value(py, vec![1, 2, 3], "s", vec!["w".into()]),
                    BTreeMap::from([(part, 2u64)]),
                );
                step.submit(any_m).unwrap();
                step.submit(raw_m).expect("any then raw in same window");
                step.poll().expect("emit batch of 2");
                let b = out.lock().unwrap();
                assert_eq!(b.len(), 1, "one batched payload");
                let list = b[0].bind(py).cast::<PyList>().expect("list payload");
                assert_eq!(list.len(), 2);
                assert_eq!(list.get_item(0).unwrap().extract::<i32>().unwrap(), 0);
                let b1: Vec<u8> = list
                    .get_item(1)
                    .unwrap()
                    .cast::<PyBytes>()
                    .unwrap()
                    .as_bytes()
                    .to_vec();
                assert_eq!(b1, vec![1, 2, 3]);
            });
        }

        #[test]
        fn watermark_forwarded_immediately_when_batch_empty() {
            let route = Route::new("s".into(), vec!["w".into()]);
            let (mut step, _out, wms) = batch_step_with_fake(route.clone(), None, None);
            let rv = crate::routes::RoutedValue {
                route,
                payload: RoutedValuePayload::make_watermark_payload(BTreeMap::new(), 0, None),
            };
            let m = Message::new_any_message(rv, BTreeMap::new());
            step.submit(m)
                .expect("watermark should go to next step when no open batch");
            assert_eq!(wms.lock().unwrap().len(), 1);
        }

        #[test]
        fn synthetic_watermark_uses_oldest_row_timestamp() {
            let route = Route::new("s".into(), vec!["w".into()]);
            let (mut step, _out, wms) = batch_step_with_fake(route, Some(2), None);
            traced_with_gil!(|py| {
                let p1 = 1i32.into_pyobject(py).unwrap().into_any().unbind();
                let p2 = 2i32.into_pyobject(py).unwrap().into_any().unbind();
                let m1 = Message::new_any_message(
                    build_routed_value_with_timestamp(py, p1, "s", vec!["w".into()], 5000.25),
                    BTreeMap::new(),
                );
                let m2 = Message::new_any_message(
                    build_routed_value_with_timestamp(py, p2, "s", vec!["w".into()], 9000.9),
                    BTreeMap::new(),
                );
                step.submit(m1).unwrap();
                step.submit(m2).unwrap();
                step.poll().unwrap();
            });
            let w = wms.lock().unwrap();
            assert_eq!(w.len(), 1);
            assert_eq!(w[0].last_message_time, Some(5000.25));
        }

        #[test]
        fn submit_rejects_while_batch_stalled_in_outbound() {
            let route = Route::new("s".into(), vec!["w".into()]);
            let (mut step, _out, _wms) = batch_step_with_fake(route.clone(), None, None);
            traced_with_gil!(|py| {
                let p_carried = 1i32.into_pyobject(py).unwrap().into_any().unbind();
                let p_next = 2i32.into_pyobject(py).unwrap().into_any().unbind();
                let carried = Message::new_any_message(
                    build_routed_value(py, p_carried, "s", vec!["w".into()]),
                    BTreeMap::new(),
                );
                step.set_stalled_outbound_for_test(Some(carried));
                let m = Message::new_any_message(
                    build_routed_value(py, p_next, "s", vec!["w".into()]),
                    BTreeMap::new(),
                );
                let err = step.submit(m).expect_err("expected backpressure");
                assert!(matches!(
                    err,
                    SubmitError::MessageRejected(MessageRejected { .. })
                ));
            });
        }
    }

    mod rejected_debounce {
        //! [`BatchStep::record_rejected_submit`]: the debounced log/metric emitted
        //! for `MessageRejected` outcomes while draining the outbound queue.

        use super::super::{BatchStep, METRIC_BATCH_SUBMIT_REJECTED, REJECTED_DEBOUNCE_INTERVAL};
        use crate::fake_strategy::FakeStrategy;
        use crate::routes::Route;
        use metrics::{
            Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
        };
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};

        /// Minimal `metrics` recorder that captures counter increments. Only the
        /// counter path is exercised by `record_rejected_submit`.
        struct CounterRecorder {
            counters: Arc<Mutex<Vec<(Key, u64)>>>,
        }

        impl Recorder for CounterRecorder {
            fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
            fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
                Counter::from_arc(Arc::new(CaptureCounter {
                    key: key.clone(),
                    counters: Arc::clone(&self.counters),
                }))
            }
            fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
                Gauge::noop()
            }
            fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
                Histogram::noop()
            }
        }

        struct CaptureCounter {
            key: Key,
            counters: Arc<Mutex<Vec<(Key, u64)>>>,
        }

        impl metrics::CounterFn for CaptureCounter {
            fn increment(&self, value: u64) {
                self.counters
                    .lock()
                    .unwrap()
                    .push((self.key.clone(), value));
            }
            fn absolute(&self, _: u64) {}
        }

        fn build_step() -> BatchStep {
            let sub = Arc::new(Mutex::new(Vec::new()));
            let wms = Arc::new(Mutex::new(Vec::new()));
            let next = FakeStrategy::new(sub, wms, false);
            BatchStep::new(
                Route::new("s".into(), vec!["w".into()]),
                None,
                None,
                "test_batch".to_string(),
                Box::new(next),
            )
        }

        /// All values emitted to the rejected-submit counter, in order.
        fn rejected_emissions(recorded: &[(Key, u64)]) -> Vec<u64> {
            recorded
                .iter()
                .filter(|(k, _)| k.name() == METRIC_BATCH_SUBMIT_REJECTED)
                .map(|(_, v)| *v)
                .collect()
        }

        #[test]
        fn first_rejection_emits_metric_immediately() {
            let counters = Arc::new(Mutex::new(Vec::new()));
            let recorder = CounterRecorder {
                counters: Arc::clone(&counters),
            };
            let _guard = metrics::set_default_local_recorder(&recorder);

            let mut step = build_step();
            step.record_rejected_submit(1.0, 3);

            assert_eq!(rejected_emissions(&counters.lock().unwrap()), vec![1]);
        }

        #[test]
        fn rejections_within_interval_are_debounced() {
            let counters = Arc::new(Mutex::new(Vec::new()));
            let recorder = CounterRecorder {
                counters: Arc::clone(&counters),
            };
            let _guard = metrics::set_default_local_recorder(&recorder);

            let mut step = build_step();
            // First emits immediately; the rest fall inside the debounce window.
            step.record_rejected_submit(1.0, 0);
            step.record_rejected_submit(1.0, 0);
            step.record_rejected_submit(1.0, 0);

            assert_eq!(
                rejected_emissions(&counters.lock().unwrap()),
                vec![1],
                "only the first rejection should emit within the interval"
            );
        }

        #[test]
        fn emits_accumulated_count_after_interval() {
            let counters = Arc::new(Mutex::new(Vec::new()));
            let recorder = CounterRecorder {
                counters: Arc::clone(&counters),
            };
            let _guard = metrics::set_default_local_recorder(&recorder);

            let mut step = build_step();
            step.record_rejected_submit(1.0, 0); // emits 1, resets the window
            step.record_rejected_submit(1.0, 0); // accumulates (pending = 1)
            step.record_rejected_submit(1.0, 0); // accumulates (pending = 2)

            // Pretend the debounce window has elapsed since the last emission.
            step.rejected_last_emitted =
                Some(Instant::now() - REJECTED_DEBOUNCE_INTERVAL - Duration::from_secs(1));
            step.record_rejected_submit(1.0, 0); // now due: emits the accumulated 3

            assert_eq!(
                rejected_emissions(&counters.lock().unwrap()),
                vec![1, 3],
                "immediate first emit, then the accumulated count once the interval passes"
            );
        }
    }
}
