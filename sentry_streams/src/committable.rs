/// Utility methods for working with Arroyo committables
use crate::routes::RoutedValue;

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::IntoPyObjectExt;

use sentry_arroyo::types::{Message, Partition, Topic};
use std::collections::BTreeMap;

/// Returns a clone of a message's committable
pub fn clone_committable(message: &Message<RoutedValue>) -> BTreeMap<Partition, u64> {
    message.committable().clone().collect()
}

/// Converts a message committable to a python dict object
pub fn convert_committable_to_py(
    py: Python<'_>,
    committable: BTreeMap<Partition, u64>,
) -> Result<Py<PyAny>, PyErr> {
    let dict = PyDict::new(py);
    for (partition, offset) in committable {
        let key = PyTuple::new(
            py,
            &[
                partition.topic.as_str().into_py_any(py)?,
                partition.index.into_py_any(py)?,
            ],
        );
        dict.set_item(key?, offset)?;
    }
    Ok(dict.into())
}

/// Converts a python dict containing a committable into a rust BTreeMap
pub fn convert_py_committable(
    py: Python<'_>,
    py_committable: Py<PyAny>,
) -> Result<BTreeMap<Partition, u64>, PyErr> {
    let mut committable = BTreeMap::new();
    let dict = py_committable.downcast_bound::<PyDict>(py)?;
    for (key, value) in dict.iter() {
        let partition = key.downcast::<PyTuple>()?;
        let topic: String = partition.get_item(0)?.extract()?;
        let index: u16 = partition.get_item(1)?.extract()?;
        let offset: u64 = value.extract()?;
        committable.insert(
            Partition {
                topic: Topic::new(&topic),
                index,
            },
            offset,
        );
    }
    Ok(committable)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::testutils::make_committable;
    use crate::utils::traced_with_gil;

    #[test]
    fn test_convert_committable_to_py_and_back() {
        pyo3::prepare_freethreaded_python();
        traced_with_gil!(|py| {
            // Prepare a committable with two partitions
            let committable = make_committable(2, 0);

            // Convert to Python object and back
            let py_obj = convert_committable_to_py(py, committable.clone()).unwrap();
            let committable_back = convert_py_committable(py, py_obj).unwrap();

            // Assert equality
            assert_eq!(committable, committable_back);
        });
    }
}
