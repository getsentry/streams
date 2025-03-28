use crate::steps::ArroyoStep;
use pyo3::types::PyList;

use pyo3::prelude::*;

#[pyclass]
pub struct ArroyoConsumer {
    source: String,
    steps: Vec<Box<dyn ArroyoStep>>,
}

#[pymethods]
impl ArroyoConsumer {
    #[new]
    fn new(source: String) -> Self {
        ArroyoConsumer {
            source,
            steps: Vec::new(),
        }
    }

    fn add_step(&mut self, step: PyObject) {
        // Assuming ArroyoStep is implemented for Python objects
        self.steps.push(Box::new(step));
    }

    fn dump(&self) {
        println!("Arroyo Consumer:");
        println!("Source: {}", self.source);
        for step in &self.steps {
            println!("Step: {:?}", step);
        }
    }
}
