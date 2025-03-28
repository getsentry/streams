use pyo3::prelude::*;
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

#[pyclass]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Route {
    /// Represents the route taken by a message in the pipeline when
    /// there are branches or multiple sources.
    ///
    /// Each pipeline step is assigned a route so it only processes
    /// messages that belong to it giving the illusion that Arroyo
    /// supports branches which it does not.
    ///
    /// The waypoints sequence contains the branches taken by the message
    /// in order following the pipeline.
    #[pyo3(get, set)]
    pub source: String,
    #[pyo3(get, set)]
    pub waypoints: Vec<String>,
}

#[pymethods]
impl Route {
    #[new]
    pub fn new(source: String, waypoints: Vec<String>) -> Self {
        Route { source, waypoints }
    }
}

#[derive(Debug)]
pub struct RoutedValue {
    pub route: Route,
    pub payload: Py<PyAny>, // Replace String with the concrete type you need
}
