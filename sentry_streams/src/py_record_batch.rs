//! Hands an Arrow `RecordBatch` to Python over the Arrow PyCapsule interface.
//!
//! Deliberately no `pyarrow` dependency: any consumer that speaks the PyCapsule
//! protocol (polars, pyarrow, duckdb, ...) can read the batch without a copy.
//!
//! See `docs/design/arrow-batch-parser.md`, phase 1.

use arrow::array::{Array, RecordBatch, RecordBatchIterator, StructArray};
use arrow::error::ArrowError;
use arrow::ffi::{to_ffi, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ffi::CStr;

/// Capsule names mandated by the Arrow PyCapsule interface. Getting one wrong is
/// not a soft failure: consumers reject the capsule with an opaque error, so they
/// are named constants and asserted in the tests.
const SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";
const ARRAY_CAPSULE_NAME: &CStr = c"arrow_array";
const STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";

/// An Arrow `RecordBatch` produced by the Rust runtime, readable from Python by
/// anything that speaks the Arrow PyCapsule interface:
///
/// ```python
/// import polars as pl
/// df = pl.DataFrame(batch)
/// ```
#[pyclass(
    name = "ArrowRecordBatch",
    module = "sentry_streams.rust_streams",
    frozen
)]
pub struct PyRecordBatch {
    pub(crate) batch: RecordBatch,
}

impl PyRecordBatch {
    // Constructed by the Arrow batch parser step (phase 4); until then only the
    // tests build one.
    #[allow(dead_code)]
    pub(crate) fn new(batch: RecordBatch) -> Self {
        Self { batch }
    }
}

fn arrow_err(e: ArrowError) -> PyErr {
    PyRuntimeError::new_err(format!("Arrow C data interface export failed: {e}"))
}

#[pymethods]
impl PyRecordBatch {
    #[getter]
    fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    fn __repr__(&self) -> String {
        format!(
            "ArrowRecordBatch(num_rows={}, num_columns={})",
            self.batch.num_rows(),
            self.batch.num_columns()
        )
    }

    /// Export as a single Arrow array (a struct array, one field per column).
    ///
    /// `requested_schema` is accepted and ignored: the PyCapsule interface allows
    /// a producer to return its native schema when it cannot perform the
    /// requested cast, and we never cast.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
        let _ = requested_schema;

        let struct_array = StructArray::from(self.batch.clone());
        let (ffi_array, ffi_schema) = to_ffi(&struct_array.to_data()).map_err(arrow_err)?;

        // The capsule takes ownership; FFI_ArrowSchema/FFI_ArrowArray's Drop
        // invokes the C release callback, so no manual destructor is needed.
        let schema_capsule = PyCapsule::new_with_value(py, ffi_schema, SCHEMA_CAPSULE_NAME)?;
        let array_capsule = PyCapsule::new_with_value(py, ffi_array, ARRAY_CAPSULE_NAME)?;
        Ok((schema_capsule, array_capsule))
    }

    /// Export just the schema, for consumers that inspect before reading.
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let ffi_schema =
            FFI_ArrowSchema::try_from(self.batch.schema().as_ref()).map_err(arrow_err)?;
        PyCapsule::new_with_value(py, ffi_schema, SCHEMA_CAPSULE_NAME)
    }

    /// Export as a stream of exactly one batch.
    ///
    /// Table-level consumers (`pl.DataFrame(obj)`, `pa.table(obj)`) look for this
    /// rather than `__arrow_c_array__`, which is why both exist.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;

        let batch = self.batch.clone();
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));

        PyCapsule::new_with_value(py, stream, STREAM_CAPSULE_NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Int64Array, MapBuilder, MapFieldNames, StringArray, StringBuilder,
    };
    use arrow::datatypes::Schema;
    use arrow::ffi::from_ffi;
    use arrow::ffi_stream::ArrowArrayStreamReader;
    use arrow::record_batch::RecordBatchReader;
    use pyo3::types::PyCapsuleMethods;
    use std::sync::Arc;

    fn capsule_name(capsule: &Bound<'_, PyCapsule>) -> &'static CStr {
        // SAFETY: the name is a `&'static CStr` we set ourselves and no Python
        // code has had the chance to rename the capsule.
        unsafe { capsule.name().unwrap().unwrap().as_cstr() }
    }

    /// Consume the capsules the way a real consumer does: move the array out and
    /// leave the exported struct released, so double-release bugs would show up.
    fn import_array(
        schema_capsule: &Bound<'_, PyCapsule>,
        array_capsule: &Bound<'_, PyCapsule>,
    ) -> RecordBatch {
        let data = unsafe {
            let schema_ptr = schema_capsule
                .pointer_checked(Some(SCHEMA_CAPSULE_NAME))
                .unwrap()
                .as_ptr() as *const FFI_ArrowSchema;
            let array_ptr = array_capsule
                .pointer_checked(Some(ARRAY_CAPSULE_NAME))
                .unwrap()
                .as_ptr() as *mut arrow::ffi::FFI_ArrowArray;
            let array = std::ptr::replace(array_ptr, arrow::ffi::FFI_ArrowArray::empty());
            from_ffi(array, &*schema_ptr).unwrap()
        };
        RecordBatch::from(StructArray::from(data))
    }

    fn import_schema(capsule: &Bound<'_, PyCapsule>) -> Schema {
        unsafe {
            let ptr = capsule
                .pointer_checked(Some(SCHEMA_CAPSULE_NAME))
                .unwrap()
                .as_ptr() as *const FFI_ArrowSchema;
            Schema::try_from(&*ptr).unwrap()
        }
    }

    /// Two scalar columns plus a `Map<Utf8, Utf8>`, so the nested case is
    /// exercised by every test rather than only by a dedicated one.
    fn sample_batch() -> RecordBatch {
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let names: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")]));

        let mut attrs = MapBuilder::new(
            Some(MapFieldNames {
                entry: "entries".to_string(),
                key: "key".to_string(),
                value: "value".to_string(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        );
        // row 0: two entries, row 1: none, row 2: one entry
        attrs.keys().append_value("k1");
        attrs.values().append_value("v1");
        attrs.keys().append_value("k2");
        attrs.values().append_value("v2");
        attrs.append(true).unwrap();
        attrs.append(true).unwrap();
        attrs.keys().append_value("k3");
        attrs.values().append_value("v3");
        attrs.append(true).unwrap();
        let attrs: ArrayRef = Arc::new(attrs.finish());

        RecordBatch::try_from_iter(vec![("id", ids), ("name", names), ("attrs", attrs)]).unwrap()
    }

    fn py_batch(py: Python<'_>) -> Py<PyRecordBatch> {
        Py::new(py, PyRecordBatch::new(sample_batch())).unwrap()
    }

    #[test]
    fn exposes_shape_to_python() {
        crate::testutils::initialize_python();
        Python::attach(|py| {
            let rb = py_batch(py);
            let b = rb.bind(py);
            assert_eq!(
                b.getattr("num_rows").unwrap().extract::<usize>().unwrap(),
                3
            );
            assert_eq!(
                b.getattr("num_columns")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                3
            );
            let repr = b.repr().unwrap().extract::<String>().unwrap();
            assert!(repr.contains("ArrowRecordBatch"), "got {repr}");
            assert!(repr.contains('3'), "repr should mention the shape: {repr}");
        });
    }

    #[test]
    fn arrow_c_array_round_trips_through_ffi() {
        crate::testutils::initialize_python();
        Python::attach(|py| {
            let rb = py_batch(py);
            let (schema_capsule, array_capsule) = rb.get().__arrow_c_array__(py, None).unwrap();

            assert_eq!(capsule_name(&schema_capsule), SCHEMA_CAPSULE_NAME);
            assert_eq!(capsule_name(&array_capsule), ARRAY_CAPSULE_NAME);

            // Import the capsules back and compare against the original batch.
            let round_tripped = import_array(&schema_capsule, &array_capsule);
            assert_eq!(round_tripped, sample_batch());
        });
    }

    #[test]
    fn arrow_c_schema_describes_the_batch() {
        crate::testutils::initialize_python();
        Python::attach(|py| {
            let rb = py_batch(py);
            let capsule = rb.get().__arrow_c_schema__(py).unwrap();
            assert_eq!(capsule_name(&capsule), SCHEMA_CAPSULE_NAME);

            assert_eq!(&import_schema(&capsule), sample_batch().schema().as_ref());
        });
    }

    #[test]
    fn arrow_c_stream_yields_the_batch() {
        crate::testutils::initialize_python();
        Python::attach(|py| {
            let rb = py_batch(py);
            let capsule = rb.get().__arrow_c_stream__(py, None).unwrap();
            assert_eq!(capsule_name(&capsule), STREAM_CAPSULE_NAME);

            let ptr = capsule
                .pointer_checked(Some(STREAM_CAPSULE_NAME))
                .unwrap()
                .as_ptr() as *mut FFI_ArrowArrayStream;
            let mut reader = unsafe { ArrowArrayStreamReader::from_raw(ptr) }.unwrap();
            assert_eq!(reader.schema().as_ref(), sample_batch().schema().as_ref());
            let batch = reader.next().unwrap().unwrap();
            assert_eq!(batch, sample_batch());
            assert!(
                reader.next().is_none(),
                "stream must hold exactly one batch"
            );
        });
    }

    /// Exporting must not consume the batch: the object stays usable, which is
    /// what a Python caller passing it to two consumers would expect.
    #[test]
    fn can_be_exported_twice() {
        crate::testutils::initialize_python();
        Python::attach(|py| {
            let rb = py_batch(py);
            let _first = rb.get().__arrow_c_array__(py, None).unwrap();
            let (schema_capsule, array_capsule) = rb.get().__arrow_c_array__(py, None).unwrap();

            assert_eq!(
                import_array(&schema_capsule, &array_capsule),
                sample_batch()
            );
        });
    }

    /// `requested_schema` is accepted and ignored; the protocol allows returning
    /// the native schema when the requested cast is unsupported.
    #[test]
    fn requested_schema_is_ignored_not_rejected() {
        crate::testutils::initialize_python();
        Python::attach(|py| {
            let rb = py_batch(py);
            let requested = rb.get().__arrow_c_schema__(py).unwrap().into_any();
            assert!(rb
                .get()
                .__arrow_c_array__(py, Some(requested.clone()))
                .is_ok());
            assert!(rb.get().__arrow_c_stream__(py, Some(requested)).is_ok());
        });
    }

    /// The real acceptance criterion: an independent Arrow implementation reads
    /// our batch with the right values, names and dtypes.
    #[test]
    fn polars_reads_the_batch() {
        crate::testutils::initialize_python();
        Python::attach(|py| {
            // polars is a declared runtime dependency of this package, so a
            // missing import is a broken environment, not a reason to skip.
            let pl = py.import("polars").expect("polars must be importable");
            let rb = py_batch(py);
            let df = pl.call_method1("DataFrame", (rb,)).unwrap();

            let columns: Vec<String> = df.getattr("columns").unwrap().extract().unwrap();
            assert_eq!(columns, vec!["id", "name", "attrs"]);
            assert_eq!(
                df.call_method0("__len__")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                3
            );

            let ids: Vec<i64> = df
                .get_item("id")
                .unwrap()
                .call_method0("to_list")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(ids, vec![1, 2, 3]);

            let names: Vec<Option<String>> = df
                .get_item("name")
                .unwrap()
                .call_method0("to_list")
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(
                names,
                vec![Some("a".to_string()), None, Some("c".to_string())]
            );

            let dtype = df
                .get_item("id")
                .unwrap()
                .getattr("dtype")
                .unwrap()
                .str()
                .unwrap()
                .extract::<String>()
                .unwrap();
            assert_eq!(dtype, "Int64");
        });
    }

    #[test]
    fn empty_batch_keeps_its_schema() {
        crate::testutils::initialize_python();
        Python::attach(|py| {
            let schema = sample_batch().schema();
            let empty = RecordBatch::new_empty(schema.clone());
            let rb = Py::new(py, PyRecordBatch::new(empty)).unwrap();
            assert_eq!(rb.get().num_rows(), 0);

            let capsule = rb.get().__arrow_c_schema__(py).unwrap();
            assert_eq!(&import_schema(&capsule), schema.as_ref());
        });
    }
}
