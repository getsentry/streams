/// Base trait for all Rust callback functions that can be called from the streaming pipeline
/// Macro to create a Rust map function that can be called from Python
/// Usage: rust_map_function!(MyFunction, InputType, OutputType, |msg: Message<InputType>| -> Message<OutputType> { ... });
#[macro_export]
macro_rules! rust_map_function {
    ($name:ident, $input_type:ty, $output_type:ty, $transform_fn:expr) => {
        #[pyo3::pyclass]
        pub struct $name {
            rust_fn_ptr: usize,
        }

        #[pyo3::pymethods]
        impl $name {
            #[new]
            pub fn new() -> Self {
                Self {
                    rust_fn_ptr: Self::get_rust_fn_ptr(),
                }
            }

            #[pyo3(name = "__call__")]
            pub fn call(&self, _msg: pyo3::Py<pyo3::PyAny>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
                // Python fallback - not implemented for performance reasons
                Err(pyo3::PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "This function should be called directly from Rust for performance"
                ))
            }

            pub fn get_rust_function_pointer(&self) -> usize {
                self.rust_fn_ptr
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
        }

        // The actual Rust implementation function (unique name per macro invocation)
        paste::paste! {
            extern "C" fn [<rust_map_impl_ $name:snake>](
                input: *const $crate::ffi::Message<$input_type>,
            ) -> *const $crate::ffi::Message<$output_type> {
                let input_msg = unsafe {
                    $crate::ffi::ptr_to_message(input)
                };

                let transform_fn: fn($crate::ffi::Message<$input_type>) -> $crate::ffi::Message<$output_type> = $transform_fn;
                let output_msg = transform_fn(input_msg);

                $crate::ffi::message_to_ptr(output_msg)
            }
        }

        impl $name {
            fn get_rust_fn_ptr() -> usize {
                paste::paste! {
                    [<rust_map_impl_ $name:snake>] as usize
                }
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
        pub struct $name {
            rust_fn_ptr: usize,
        }

        #[pyo3::pymethods]
        impl $name {
            #[new]
            pub fn new() -> Self {
                Self {
                    rust_fn_ptr: Self::get_rust_fn_ptr(),
                }
            }

            #[pyo3(name = "__call__")]
            pub fn call(&self, _msg: pyo3::Py<pyo3::PyAny>) -> pyo3::PyResult<bool> {
                // Python fallback - not implemented for performance reasons
                Err(
                    pyo3::PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                        "This function should be called directly from Rust for performance",
                    ),
                )
            }

            pub fn get_rust_function_pointer(&self) -> usize {
                self.rust_fn_ptr
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
        }

        // The actual Rust implementation function (unique name per macro invocation)
        paste::paste! {
            extern "C" fn [<rust_filter_impl_ $name:snake>](
                input: *const $crate::ffi::Message<$input_type>,
            ) -> bool {
                let input_msg = unsafe {
                    $crate::ffi::ptr_to_message(input)
                };

                let filter_fn: fn($crate::ffi::Message<$input_type>) -> bool = $filter_fn;
                filter_fn(input_msg)
            }
        }

        impl $name {
            fn get_rust_fn_ptr() -> usize {
                paste::paste! {
                    [<rust_filter_impl_ $name:snake>] as usize
                }
            }
        }
    };
}

/// Generic macro for creating any type of Rust callback function
/// Usage: rust_callback_function!(map, MyFunction, InputType, OutputType, |msg| { ... });
#[macro_export]
macro_rules! rust_callback_function {
    (map, $name:ident, $input_type:ty, $output_type:ty, $transform_fn:expr) => {
        $crate::rust_map_function!($name, $input_type, $output_type, $transform_fn);
    };

    (filter, $name:ident, $input_type:ty, $filter_fn:expr) => {
        $crate::rust_filter_function!($name, $input_type, $filter_fn);
    };
}
