/// Macro to create a Rust map function that can be called from Python
/// Usage: rust_map_function!(MyFunction, InputType, OutputType, |msg: Message<InputType>| -> Message<OutputType> { ... });
#[macro_export]
macro_rules! rust_map_function {
    ($name:ident, $input_type:ty, $output_type:ty, $transform_fn:expr) => {
        #[pyo3::pyclass]
        pub struct $name;

        #[pyo3::pymethods]
        impl $name {
            #[new]
            pub fn new() -> Self {
                Self
            }

            #[pyo3(name = "__call__")]
            pub fn call(
                &self,
                py: pyo3::Python<'_>,
                py_msg: pyo3::Py<pyo3::PyAny>,
            ) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                // Convert Python message to typed Rust message
                let rust_msg = $crate::convert_py_message_to_rust::<$input_type>(py, &py_msg)?;

                // Release GIL and call Rust function
                let result_msg = py.allow_threads(|| {
                    let transform_fn: fn(
                        $crate::message::Message<$input_type>,
                    ) -> $crate::message::Message<$output_type> = $transform_fn;
                    transform_fn(rust_msg)
                });

                // Convert result back to Python message
                $crate::convert_rust_message_to_py(py, result_msg)
            }

            pub fn input_type(&self) -> &'static str {
                std::any::type_name::<$input_type>()
            }

            pub fn output_type(&self) -> &'static str {
                std::any::type_name::<$output_type>()
            }

            pub fn callback_type(&self) -> &'static str {
                "map"
            }

            pub fn is_rust_function(&self) -> bool {
                true
            }
        }
    };
}

/// Macro to create a Rust filter function that can be called from Python
/// Usage: rust_filter_function!(MyFilter, InputType, |msg: Message<InputType>| -> bool { ... });
#[macro_export]
macro_rules! rust_filter_function {
    ($name:ident, $input_type:ty, $filter_fn:expr) => {
        #[pyo3::pyclass]
        pub struct $name;

        #[pyo3::pymethods]
        impl $name {
            #[new]
            pub fn new() -> Self {
                Self
            }

            #[pyo3(name = "__call__")]
            pub fn call(
                &self,
                py: pyo3::Python<'_>,
                py_msg: pyo3::Py<pyo3::PyAny>,
            ) -> pyo3::PyResult<bool> {
                // Convert Python message to typed Rust message
                let rust_msg = $crate::convert_py_message_to_rust::<$input_type>(py, &py_msg)?;

                // Release GIL and call Rust function
                let result = py.allow_threads(|| {
                    let filter_fn: fn($crate::message::Message<$input_type>) -> bool = $filter_fn;
                    filter_fn(rust_msg)
                });

                Ok(result)
            }

            pub fn input_type(&self) -> &'static str {
                std::any::type_name::<$input_type>()
            }

            pub fn output_type(&self) -> &'static str {
                "bool"
            }

            pub fn callback_type(&self) -> &'static str {
                "filter"
            }

            pub fn is_rust_function(&self) -> bool {
                true
            }
        }
    };
}
