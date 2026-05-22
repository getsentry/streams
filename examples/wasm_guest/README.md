# sentry_streams WASM guest example

Sample [componentize-py](https://github.com/bytecodealliance/componentize-py) guest for the
`sentry-streams:processor` WIT world used by `WasmProcessor` in the Rust Arroyo adapter.

## Build

```bash
make build
```

Produces `dist/plugin.wasm`. The WIT contract lives in
[`../../sentry_streams/wit/processor.wit`](../../sentry_streams/wit/processor.wit).

## Guest behavior

- `submit`: buffers the message for the next `poll`
- `submit_watermark`: buffers the watermark for the next `poll`
- `poll`: returns all buffered outputs (messages and watermarks), or `None` if empty

Uses the host `log` import for tracing.
