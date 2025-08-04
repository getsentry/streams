use std::ffi::OsString;

use rust_streams::run::{run, PhysicalPlanConfig, PyConfig};

fn main() {
    run(
        PyConfig {
            exec_path: OsString::from(
                "/Users/johnyang/code/streams/sentry_streams/.venv/bin/python",
            ),
            module_paths: vec![
                OsString::from(
                    "/Users/johnyang/code/streams/sentry_streams/.venv/bin/python",
                ),
                OsString::from(
                    "/opt/homebrew/Cellar/python@3.11/3.11.12_1/Frameworks/Python.framework/Versions/3.11/lib/python311.zip"
                ),
                OsString::from(
                    "/opt/homebrew/Cellar/python@3.11/3.11.12_1/Frameworks/Python.framework/Versions/3.11/lib/python3.11"
                ),
                OsString::from(
                    "/opt/homebrew/Cellar/python@3.11/3.11.12_1/Frameworks/Python.framework/Versions/3.11/lib/python3.11/lib-dynload"
                ),
                OsString::from(
                    "/Users/johnyang/code/streams/sentry_streams/.venv/lib/python3.11/site-packages"
                ),
                OsString::from("/Users/johnyang/code/streams/sentry_streams")
            ],
        },
        PhysicalPlanConfig {
            adapter_name: "rust_arroyo".into(),
            config_file: OsString::from("/Users/johnyang/code/streams/sentry_streams/sentry_streams/deployment_config/simple_map_filter.yaml"),
            segment_id: Some("0".into()),
            application_name: "/Users/johnyang/code/streams/sentry_streams/sentry_streams/examples/simple_map_filter.py".into(),
        },
    )
    .unwrap();
}
