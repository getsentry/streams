use crate::messages::PyStreamingMessage;
use pyo3::{pyclass, pymethods};
use serde::{Deserialize, Serialize};

/// Represents the route taken by a message in the pipeline when
/// there are branches or multiple sources.
///
/// Each pipeline step is assigned a route so it only processes
/// messages that belong to it giving the illusion that Arroyo
/// supports branches which it does not.
///
/// The waypoints sequence contains the branches taken by the message
/// in order following the pipeline.
#[pyclass]
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Route {
    /// The name of the streaming source this route starts from.
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

    fn __eq__(&self, other: &Self) -> bool {
        self.source == other.source && self.waypoints == other.waypoints
    }

    fn __repr__(&self) -> String {
        format!(
            "Route(source='{}', waypoints={:?})",
            self.source, self.waypoints
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// Represents a message being passed between steps in the Arroyo
/// consumer. All messages have a Route attached to them which
/// represents the path taken by the message in the pipeline.
#[derive(Debug)]
pub struct RoutedValue {
    pub route: Route,
    pub payload: PyStreamingMessage,
}

impl RoutedValue {
    pub fn add_waypoint(mut self, waypoint: String) -> RoutedValue {
        self.route.waypoints.push(waypoint);
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::messages::{into_pyany, PyAnyMessage};

    use super::*;
    use pyo3::{types::PyBytes, Python};

    #[test]
    fn test_route_new() {
        let source = "source1".to_string();
        let waypoints = vec!["waypoint1".to_string(), "waypoint2".to_string()];
        let route = Route::new(source.clone(), waypoints.clone());

        assert_eq!(route.source, source);
        assert_eq!(route.waypoints, waypoints);
    }

    #[test]
    fn test_route_equality() {
        let route1 = Route::new("source1".to_string(), vec!["waypoint1".to_string()]);
        let route2 = Route::new("source1".to_string(), vec!["waypoint1".to_string()]);
        let route3 = Route::new("source2".to_string(), vec!["waypoint2".to_string()]);

        assert_eq!(route1, route2);
        assert_ne!(route1, route3);
    }

    #[test]
    fn test_route_str() {
        let route = Route::new("source1".to_string(), vec!["waypoint1".to_string()]);
        let string_representation = route.__str__();
        assert_eq!(
            string_representation,
            "Route(source='source1', waypoints=[\"waypoint1\"])"
        );
    }

    #[test]
    fn test_routed_value_creation() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let route = Route::new("source1".to_string(), vec!["waypoint1".to_string()]);
            let routed_value = RoutedValue {
                route: route.clone(),
                payload: PyStreamingMessage::PyAnyMessage {
                    content: into_pyany(
                        py,
                        PyAnyMessage {
                            payload: PyBytes::new(py, &[1, 2, 3]).into_any().unbind(),
                            headers: vec![],
                            timestamp: 0.0,
                            schema: None,
                        },
                    )
                    .unwrap(),
                },
            };

            assert_eq!(routed_value.route, route);
        });
    }
}
