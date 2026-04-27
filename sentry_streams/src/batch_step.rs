//! Batches `PyAnyMessage` rows on a route, then [`BatchStep`] forwards one `PyAnyMessage` with a
//! list payload and manages watermarks and backpressure to the next Arroyo strategy.
//!
//! Only `PyAnyMessage` streaming input is supported; `RawMessage` is rejected with
//! `SubmitError::InvalidMessage` when the input is a broker message.
//!
//! Python objects require the GIL to read. We keep `Py<PyAnyMessage>` per row and take the GIL
//! only when materializing the batched message on flush, after [`Message::into_payload`].
use crate::messages::{into_pyany, PyAnyMessage, PyStreamingMessage, RoutedValuePayload};
use crate::routes::{Route, RoutedValue};
use crate::time_helpers::current_epoch;
use crate::utils::traced_with_gil;
use pyo3::prelude::*;
use pyo3::types::PyList;
use sentry_arroyo::processing::strategies::{
    merge_commit_request, CommitRequest, MessageRejected, ProcessingStrategy, StrategyError,
    SubmitError,
};
use sentry_arroyo::types::{InnerMessage, Message, Partition};
use sentry_arroyo::utils::timing::Deadline;
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn invalid_message_submit_error(message: &Message<RoutedValue>) -> SubmitError<RoutedValue> {
    match &message.inner_message {
        InnerMessage::BrokerMessage(broker_message) => {
            SubmitError::InvalidMessage(broker_message.into())
        }
        InnerMessage::AnyMessage(..) => {
            panic!("BatchStep: invalid message on AnyMessage; Arroyo cannot surface InvalidMessage")
        }
    }
}

// --- Batch: accumulate `Py<PyAnyMessage>`; GIL on flush when building the list payload ---

/// Count- and/or time-based window of `PyAnyMessage` rows for one route. The first row is added
/// via [`Batch::from_first_row`]; more rows with [`Batch::append_row`].
pub(crate) struct Batch {
    route: Route,
    max_batch_size: Option<usize>,
    max_batch_time: Option<Duration>,
    rows: Vec<Py<PyAnyMessage>>,
    batch_deadline: Option<Deadline>,
    batch_offsets: BTreeMap<Partition, u64>,
}

impl Batch {
    pub fn from_first_row(
        route: Route,
        max_batch_size: Option<usize>,
        max_batch_time: Option<Duration>,
        first: Py<PyAnyMessage>,
        initial_committable: BTreeMap<Partition, u64>,
    ) -> Self {
        let deadline = max_batch_time.map(Deadline::new);
        Self {
            route,
            max_batch_size,
            max_batch_time,
            rows: vec![first],
            batch_deadline: deadline,
            batch_offsets: initial_committable,
        }
    }

    pub fn append_row(&mut self, row: Py<PyAnyMessage>) {
        self.rows.push(row);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn apply_committable_from(&mut self, message: &Message<RoutedValue>) {
        for (p, o) in message.committable() {
            self.batch_offsets
                .entry(p)
                .and_modify(|e| *e = (*e).max(o))
                .or_insert(o);
        }
    }

    /// Whether the current window is complete by size and/or time.
    pub fn should_flush(&self) -> bool {
        if self.is_empty() {
            return false;
        }
        if self.max_batch_size.is_some_and(|m| self.len() >= m) {
            return true;
        }
        if let (Some(_), Some(d)) = (&self.max_batch_time, &self.batch_deadline) {
            if d.has_elapsed() {
                return true;
            }
        }
        false
    }

    pub fn current_offsets_snapshot(&self) -> BTreeMap<Partition, u64> {
        self.batch_offsets.clone()
    }

    /// GIL: build list payload from stored `Py<PyAnyMessage>` rows, wrap in a batched `Message`.
    pub fn build_stacked_message(&self) -> Result<Message<RoutedValue>, StrategyError> {
        if self.rows.is_empty() {
            return Err(StrategyError::Other("Batch: empty window".into()));
        }
        let route = self.route.clone();
        let committable = self.batch_offsets.clone();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let content = traced_with_gil!(|py| -> PyResult<Py<PyAnyMessage>> {
            let first_schema = self.rows[0].bind(py).borrow().schema.clone();
            let py_items: Vec<Py<PyAny>> = self
                .rows
                .iter()
                .map(|pm| pm.bind(py).borrow().payload.clone_ref(py))
                .collect();
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

// --- BatchStep: Arroyo strategy — watermarks, backpressure, owns `Option<Batch>` ---

pub struct BatchStep {
    next_step: Box<dyn ProcessingStrategy<RoutedValue>>,

    route: Route,
    max_batch_size: Option<usize>,
    max_batch_time: Option<Duration>,
    /// `None` until the first `PyAnyMessage` in a window; then the open batch.
    batch: Option<Batch>,
    /// Watermarks received while the current batch window is open; taken when emitting after batch.
    watermark_buffer: Vec<Message<RoutedValue>>,

    message_carried_over: Option<Message<RoutedValue>>,
    pending_downstream: VecDeque<Message<RoutedValue>>,
    commit_request_carried_over: Option<CommitRequest>,
}

impl BatchStep {
    pub fn new(
        route: Route,
        max_batch_size: Option<usize>,
        max_batch_time: Option<Duration>,
        next_step: Box<dyn ProcessingStrategy<RoutedValue>>,
    ) -> Self {
        Self {
            next_step,
            route,
            max_batch_size,
            max_batch_time,
            batch: None,
            watermark_buffer: Vec::new(),
            message_carried_over: None,
            pending_downstream: VecDeque::new(),
            commit_request_carried_over: None,
        }
    }

    fn is_quiesced(&self) -> bool {
        self.message_carried_over.is_none()
            && self.batch.as_ref().map_or(true, |b| b.is_empty())
            && self.pending_downstream.is_empty()
            && self.watermark_buffer.is_empty()
    }

    fn submit_with_poll(&mut self, msg: Message<RoutedValue>) -> Result<bool, StrategyError> {
        let c = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), c);
        match self.next_step.submit(msg) {
            Ok(()) => Ok(true),
            Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                self.pending_downstream.push_front(message);
                Ok(false)
            }
            Err(SubmitError::InvalidMessage(e)) => Err(e.into()),
        }
    }

    fn drain_pending_downstream(&mut self) -> Result<(), StrategyError> {
        while let Some(msg) = self.pending_downstream.pop_front() {
            if !self.submit_with_poll(msg)? {
                break;
            }
        }
        Ok(())
    }

    fn retry_carried_batch(&mut self) -> Result<(), StrategyError> {
        if let Some(msg) = self.message_carried_over.take() {
            let c = self.next_step.poll()?;
            self.commit_request_carried_over =
                merge_commit_request(self.commit_request_carried_over.take(), c);
            match self.next_step.submit(msg) {
                Ok(()) => {}
                Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                    self.message_carried_over = Some(message);
                }
                Err(SubmitError::InvalidMessage(e)) => return Err(e.into()),
            }
        }
        Ok(())
    }

    fn try_emit_batch(&mut self, force: bool) -> Result<(), StrategyError> {
        if self.message_carried_over.is_some() {
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
        let committable_for_synthetic = b.current_offsets_snapshot();
        let batch_msg = b.build_stacked_message()?;
        self.batch = None;
        let wm_after_batch: Vec<_> = std::mem::take(&mut self.watermark_buffer);

        let c = self.next_step.poll()?;
        self.commit_request_carried_over =
            merge_commit_request(self.commit_request_carried_over.take(), c);

        match self.next_step.submit(batch_msg) {
            Err(SubmitError::MessageRejected(MessageRejected { message })) => {
                self.message_carried_over = Some(message);
                self.watermark_buffer = wm_after_batch;
            }
            Err(SubmitError::InvalidMessage(e)) => {
                self.watermark_buffer = wm_after_batch;
                return Err(e.into());
            }
            Ok(()) => {
                self.emit_watermark_tail(wm_after_batch, committable_for_synthetic)?;
            }
        }
        Ok(())
    }

    fn emit_watermark_tail(
        &mut self,
        mut wm_after_batch: Vec<Message<RoutedValue>>,
        committable_for_synthetic: BTreeMap<Partition, u64>,
    ) -> Result<(), StrategyError> {
        let wmk = self.make_synthetic_watermark(committable_for_synthetic);
        wm_after_batch.push(wmk);
        while !wm_after_batch.is_empty() {
            let m = wm_after_batch.remove(0);
            if !self.submit_with_poll(m)? {
                for r in wm_after_batch.into_iter().rev() {
                    self.pending_downstream.push_front(r);
                }
                break;
            }
        }
        Ok(())
    }

    fn make_synthetic_watermark(
        &self,
        committable: BTreeMap<Partition, u64>,
    ) -> Message<RoutedValue> {
        let ts = current_epoch();
        let rv = RoutedValue {
            route: self.route.clone(),
            payload: RoutedValuePayload::make_watermark_payload(committable.clone(), ts),
        };
        Message::new_any_message(rv, committable)
    }
}

/// Public constructor used by `operators::build`.
pub fn build_batch_step(
    route: &Route,
    max_batch_size: Option<usize>,
    max_batch_time: Option<Duration>,
    next: Box<dyn ProcessingStrategy<RoutedValue>>,
) -> Box<dyn ProcessingStrategy<RoutedValue>> {
    Box::new(BatchStep::new(
        route.clone(),
        max_batch_size,
        max_batch_time,
        next,
    ))
}

impl ProcessingStrategy<RoutedValue> for BatchStep {
    fn poll(&mut self) -> Result<Option<CommitRequest>, StrategyError> {
        self.drain_pending_downstream()?;
        self.retry_carried_batch()?;
        if self.message_carried_over.is_none() {
            self.try_emit_batch(false)?;
        }
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

        match &message.payload().payload {
            RoutedValuePayload::WatermarkMessage(_) => {
                self.watermark_buffer.push(message);
                Ok(())
            }
            RoutedValuePayload::PyStreamingMessage(pysm) => {
                if matches!(pysm, PyStreamingMessage::RawMessage { .. }) {
                    return Err(invalid_message_submit_error(&message));
                }

                if self.batch.is_none() {
                    let committable: BTreeMap<Partition, u64> = message.committable().collect();
                    let inner = message.into_payload();
                    let pysm = match inner.payload {
                        RoutedValuePayload::PyStreamingMessage(m) => m,
                        _ => unreachable!(),
                    };
                    let PyStreamingMessage::PyAnyMessage { content } = pysm else {
                        unreachable!("RawMessage was rejected using &pysm before into_payload");
                    };
                    self.batch = Some(Batch::from_first_row(
                        self.route.clone(),
                        self.max_batch_size,
                        self.max_batch_time,
                        content,
                        committable,
                    ));
                    return Ok(());
                }

                let b = self.batch.as_mut().expect("open batch");
                b.apply_committable_from(&message);
                let inner = message.into_payload();
                let pysm = match inner.payload {
                    RoutedValuePayload::PyStreamingMessage(m) => m,
                    _ => unreachable!(),
                };
                let PyStreamingMessage::PyAnyMessage { content } = pysm else {
                    unreachable!("RawMessage was rejected using &pysm before into_payload");
                };
                b.append_row(content);
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
            self.drain_pending_downstream()?;
            self.retry_carried_batch()?;
            if self.message_carried_over.is_none() {
                self.try_emit_batch(true)?;
            }
            if self.is_quiesced() {
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
        //! [`Batch`] only: rows, committable merge, `should_flush`, and list materialization (GIL).

        use crate::batch_step::Batch;
        use crate::messages::{into_pyany, PyAnyMessage, PyStreamingMessage, RoutedValuePayload};
        use crate::routes::Route;
        use crate::testutils::build_routed_value;
        use crate::utils::traced_with_gil;
        use chrono::Utc;
        use pyo3::prelude::*;
        use pyo3::types::{PyAnyMethods, PyList};
        use pyo3::IntoPyObject;
        use sentry_arroyo::types::{Message, Partition, Topic};
        use std::collections::BTreeMap;

        fn route() -> Route {
            Route::new("s".into(), vec!["w".into()])
        }

        #[test]
        fn build_stacked_message_makes_list_payload() {
            traced_with_gil!(|py| {
                let r = route();
                let p1 = 1i32.into_pyobject(py).unwrap().into_any().unbind();
                let p2 = 2i32.into_pyobject(py).unwrap().into_any().unbind();
                let a1 = into_pyany(
                    py,
                    PyAnyMessage {
                        payload: p1,
                        headers: vec![],
                        timestamp: 0.0,
                        schema: None,
                    },
                )
                .unwrap();
                let a2 = into_pyany(
                    py,
                    PyAnyMessage {
                        payload: p2,
                        headers: vec![],
                        timestamp: 0.0,
                        schema: None,
                    },
                )
                .unwrap();
                let part = Partition::new(Topic::new("t"), 0);
                let mut b = Batch::from_first_row(
                    r.clone(),
                    Some(2),
                    None,
                    a1,
                    BTreeMap::from([(part, 1u64)]),
                );
                b.append_row(a2);
                let msg = b.build_stacked_message().expect("build");
                assert!(
                    msg.committable().any(|(p, o)| p == part && o == 1),
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

        /// Uses [`build_routed_value`] to build a broker `Message` for [`Batch::apply_committable_from`].
        #[test]
        fn apply_committable_merges_max_per_partition() {
            traced_with_gil!(|py| {
                let r = route();
                let p0 = 0i32.into_pyobject(py).unwrap().into_any().unbind();
                let row = into_pyany(
                    py,
                    PyAnyMessage {
                        payload: p0,
                        headers: vec![],
                        timestamp: 0.0,
                        schema: None,
                    },
                )
                .unwrap();
                let part = Partition::new(Topic::new("t"), 0);
                let mut b =
                    Batch::from_first_row(r, None, None, row, BTreeMap::from([(part, 1u64)]));
                let p_for_msg = 0i32.into_pyobject(py).unwrap().into_any().unbind();
                let py_rv = build_routed_value(py, p_for_msg, "s", vec!["w".into()]);
                let m = Message::new_broker_message(py_rv, part, 9, Utc::now());
                b.apply_committable_from(&m);
                let snap = b.current_offsets_snapshot();
                assert_eq!(snap.get(&part).copied(), Some(10u64));
            });
        }

        #[test]
        fn should_flush_when_max_batch_size_reached() {
            traced_with_gil!(|py| {
                let r = route();
                let a1 = any_row(py, 1i32);
                let a2 = any_row(py, 2i32);
                let part = Partition::new(Topic::new("t"), 0);
                let mut b =
                    Batch::from_first_row(r, Some(2), None, a1, BTreeMap::from([(part, 0u64)]));
                assert!(!b.should_flush(), "one row, limit 2");
                b.append_row(a2);
                assert!(b.should_flush(), "two rows, limit 2");
            });
        }

        fn any_row(py: pyo3::Python<'_>, v: i32) -> pyo3::Py<PyAnyMessage> {
            let p = v.into_pyobject(py).unwrap().into_any().unbind();
            into_pyany(
                py,
                PyAnyMessage {
                    payload: p,
                    headers: vec![],
                    timestamp: 0.0,
                    schema: None,
                },
            )
            .unwrap()
        }
    }

    mod step {
        //! [`BatchStep`] as [`ProcessingStrategy`]: routing, raw rejection, handoff to the next step.

        use super::super::{BatchStep, Message};
        use crate::fake_strategy::FakeStrategy;
        use crate::testutils::{build_raw_routed_value, build_routed_value};
        use crate::utils::traced_with_gil;
        use chrono::Utc;
        use pyo3::prelude::*;
        use pyo3::IntoPyObject;
        use sentry_arroyo::processing::strategies::{
            InvalidMessage, ProcessingStrategy, SubmitError,
        };
        use sentry_arroyo::types::{Partition, Topic};
        use std::collections::BTreeMap;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        use crate::routes::Route;

        fn batch_step_with_fake(
            route: Route,
            max_n: Option<usize>,
            max_t: Option<Duration>,
        ) -> (BatchStep, Arc<Mutex<Vec<Py<PyAny>>>>) {
            let sub = Arc::new(Mutex::new(Vec::new()));
            let next = FakeStrategy::new(sub.clone(), Arc::default(), false);
            let step = BatchStep::new(route, max_n, max_t, Box::new(next));
            (step, sub)
        }

        #[test]
        fn forwards_mismatched_route_to_next_strategy() {
            let step_route = Route::new("a".into(), vec![]);
            let (mut step, captured) = batch_step_with_fake(step_route, None, None);
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
            let (mut step, out) = batch_step_with_fake(route, Some(2), None);
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
        fn submit_rejects_raw_message_after_pyany() {
            let route = Route::new("s".into(), vec!["w".into()]);
            let (mut step, _out) = batch_step_with_fake(route, Some(10), None);
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
                let err = step.submit(raw_m);
                let SubmitError::InvalidMessage(InvalidMessage { offset, .. }) =
                    err.expect_err("raw")
                else {
                    panic!("expected InvalidMessage");
                };
                assert_eq!(offset, 2);
            });
        }

        #[test]
        fn submit_rejects_leading_raw_message() {
            let route = Route::new("s".into(), vec!["w".into()]);
            let (mut step, _out) = batch_step_with_fake(route, Some(10), None);
            let part = Partition::new(Topic::new("topic"), 0);
            traced_with_gil!(|py| {
                let raw_m = Message::new_broker_message(
                    build_raw_routed_value(py, vec![9], "s", vec!["w".into()]),
                    part,
                    0,
                    Utc::now(),
                );
                let err = step.submit(raw_m);
                let SubmitError::InvalidMessage(InvalidMessage { offset, .. }) =
                    err.expect_err("raw first")
                else {
                    panic!("expected InvalidMessage");
                };
                assert_eq!(offset, 0);
            });
        }
    }
}
