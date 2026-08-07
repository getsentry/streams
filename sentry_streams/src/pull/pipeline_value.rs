use std::any::Any;
use std::fmt;

use pyo3::prelude::*;
use sentry_arroyo::backends::kafka::types::KafkaPayload;
use sentry_arroyo::processing::stream::{PipelineEnvelope, StageResult};

/// The universal payload type for pull-based pipeline stages in streams.
///
/// Stages are `Stage<In = PipelineValue, Out = PipelineValue>`, allowing
/// dynamic pipeline construction from the Python DSL without compile-time
/// generics for every pipeline shape.
///
/// Each stage knows which variant it expects and uses the downcast helpers
/// on `PipelineEnvelope<PipelineValue>` to unwrap. A variant mismatch is
/// a pipeline construction bug and results in `StageResult::Fail`.
pub enum PipelineValue {
    /// Raw Kafka payload — from source, pre-parse.
    Raw(KafkaPayload),

    /// Typed Rust data.
    /// Downcast via `PipelineEnvelope::downcast_rust::<T>()`.
    Rust(Box<dyn Any + Send + Sync>),

    /// Python heap object.
    Python(Py<PyAny>),
}

impl fmt::Debug for PipelineValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineValue::Raw(_) => write!(f, "PipelineValue::Raw(...)"),
            PipelineValue::Rust(_) => write!(f, "PipelineValue::Rust(...)"),
            PipelineValue::Python(_) => write!(f, "PipelineValue::Python(...)"),
        }
    }
}

/// Error returned when a stage receives an unexpected PipelineValue variant.
#[derive(Debug)]
pub struct DowncastError {
    expected: &'static str,
    actual: &'static str,
}

impl fmt::Display for DowncastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Pipeline misconfigured: expected PipelineValue::{}, got PipelineValue::{}",
            self.expected, self.actual
        )
    }
}

impl std::error::Error for DowncastError {}

impl PipelineValue {
    fn variant_name(&self) -> &'static str {
        match self {
            PipelineValue::Raw(_) => "Raw",
            PipelineValue::Rust(_) => "Rust",
            PipelineValue::Python(_) => "Python",
        }
    }
}

/// Extension trait for downcasting PipelineEnvelope<PipelineValue>.
pub trait PipelineValueCaster {
    /// Downcast to a Raw kafka payload.
    fn downcast_raw(self) -> Result<PipelineEnvelope<KafkaPayload>, StageResult<PipelineValue>>;

    /// Downcast to a typed Rust value.
    fn downcast_rust<T: 'static + Send + Sync>(
        self,
    ) -> Result<PipelineEnvelope<T>, StageResult<PipelineValue>>;

    /// Downcast to a Python object.
    fn downcast_python(self) -> Result<PipelineEnvelope<Py<PyAny>>, StageResult<PipelineValue>>;
}

impl PipelineValueCaster for PipelineEnvelope<PipelineValue> {
    fn downcast_raw(self) -> Result<PipelineEnvelope<KafkaPayload>, StageResult<PipelineValue>> {
        match self.payload {
            PipelineValue::Raw(kp) => Ok(PipelineEnvelope::new(kp, self.metadata, self.raw)),
            other => Err(StageResult::Fail(Box::new(DowncastError {
                expected: "Raw",
                actual: other.variant_name(),
            }))),
        }
    }

    fn downcast_rust<T: 'static + Send + Sync>(
        self,
    ) -> Result<PipelineEnvelope<T>, StageResult<PipelineValue>> {
        match self.payload {
            PipelineValue::Rust(boxed) => match boxed.downcast::<T>() {
                Ok(val) => Ok(PipelineEnvelope::new(*val, self.metadata, self.raw)),
                Err(_) => Err(StageResult::Fail(Box::new(DowncastError {
                    expected: std::any::type_name::<T>(),
                    actual: "Rust(wrong type)",
                }))),
            },
            other => Err(StageResult::Fail(Box::new(DowncastError {
                expected: "Rust",
                actual: other.variant_name(),
            }))),
        }
    }

    fn downcast_python(self) -> Result<PipelineEnvelope<Py<PyAny>>, StageResult<PipelineValue>> {
        match self.payload {
            PipelineValue::Python(obj) => Ok(PipelineEnvelope::new(obj, self.metadata, self.raw)),
            other => Err(StageResult::Fail(Box::new(DowncastError {
                expected: "Python",
                actual: other.variant_name(),
            }))),
        }
    }
}

/// Helper to wrap a typed envelope back into a PipelineValue envelope.
pub trait IntoPipelineValue<T> {
    fn into_pipeline_value(self) -> PipelineEnvelope<PipelineValue>;
}

impl IntoPipelineValue<KafkaPayload> for PipelineEnvelope<KafkaPayload> {
    fn into_pipeline_value(self) -> PipelineEnvelope<PipelineValue> {
        self.map_payload(PipelineValue::Raw)
    }
}
