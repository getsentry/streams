use crate::routes::{Route, RoutedValue};
use pyo3::prelude::*;
use sentry_arroyo::processing::strategies::{
    ProcessingStrategy, ProcessingStrategyFactory, SubmitError,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub trait ArroyoStep {
    /// Represents a primitive in Arroyo. This is the intermediate representation
    /// the Arroyo adapter uses to build the application in reverse order with
    /// respect to how the steps are wired up in the pipeline.
    ///
    /// Arroyo consumers have to be built wiring up strategies from the end to
    /// the beginning. The streaming pipeline is defined from the beginning to
    /// the end, so when building the Arroyo application we need to reverse the
    /// order of the steps.

    fn build(
        &self,
        next: Arc<dyn ProcessingStrategy<FilteredPayloadOrRoutedValue>>,
    ) -> Arc<dyn ProcessingStrategy<FilteredPayloadOrRoutedValue>>;
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FilteredPayload;

#[pyclass]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum FilteredPayloadOrRoutedValue {
    FilteredPayload(FilteredPayload),
    RoutedValue(RoutedValue), // Replace String with the concrete type you need
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct MapStep {
    pub route: Route,
    pub function: String,
}

#[pymethods]
impl MapStep {
    #[new]
    pub fn new(route: Route, function: String) -> Self {
        MapStep { route, function }
    }

    pub fn build(
        &self,
        next: Arc<dyn ProcessingStrategy<FilteredPayloadOrRoutedValue>>,
    ) -> Arc<dyn ProcessingStrategy<FilteredPayloadOrRoutedValue>> {
        // Method body left empty
        next
    }
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct FilterStep {
    pub route: Route,
    pub function: String,
}

#[pymethods]
impl FilterStep {
    #[new]
    pub fn new(route: Route, function: String) -> Self {
        FilterStep { route, function }
    }

    pub fn build(
        &self,
        next: Arc<dyn ProcessingStrategy<FilteredPayloadOrRoutedValue>>,
    ) -> Arc<dyn ProcessingStrategy<FilteredPayloadOrRoutedValue>> {
        // Method body left empty
        next
    }
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct StreamSinkStep {
    pub route: Route,
    pub producer: Producer,
    pub topic_name: String,
}

#[pymethods]
impl StreamSinkStep {
    #[new]
    pub fn new(route: Route, producer: Producer, topic_name: String) -> Self {
        StreamSinkStep {
            route,
            producer,
            topic_name,
        }
    }

    pub fn build(
        &self,
        next: Arc<dyn ProcessingStrategy<FilteredPayloadOrRoutedValue>>,
    ) -> Arc<dyn ProcessingStrategy<FilteredPayloadOrRoutedValue>> {
        // Method body left empty
        next
    }
}

#[pymodule]
pub fn rust_steps(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<FilteredPayload>()?;
    m.add_class::<MapStep>()?;
    m.add_class::<FilterStep>()?;
    m.add_class::<StreamSinkStep>()?;
    Ok(())
}
