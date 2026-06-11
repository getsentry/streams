# WASM passthrough test guest

Minimal Rust component used as `tests/fixtures/wasm_passthrough.wasm` for `WasmProcessor` unit tests.
It does not embed Python (unlike the `componentize-py` guest under `examples/wasm_guest/`).

## Build

```bash
make -C tests/wasm_passthrough_guest build
```
