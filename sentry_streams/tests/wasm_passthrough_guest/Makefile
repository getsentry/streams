.PHONY: build

build:
	CARGO_TARGET_DIR=./target cargo build --release
	cp target/wasm32-wasip2/release/wasm_passthrough_guest.wasm ../fixtures/wasm_passthrough.wasm
