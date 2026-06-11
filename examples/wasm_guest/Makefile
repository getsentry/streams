WIT_DIR := wit
SRC_DIR := src
DIST_DIR := dist
WASM := $(DIST_DIR)/plugin.wasm

.PHONY: bindings build clean

bindings:
	uv run componentize-py -d $(WIT_DIR) -w plugin bindings $(SRC_DIR)

$(WASM): $(SRC_DIR)/guest_impl.py $(WIT_DIR)/processor.wit
	@mkdir -p $(DIST_DIR)
	$(MAKE) bindings
	uv run componentize-py -d $(WIT_DIR) -w plugin componentize -p $(SRC_DIR) guest_impl -o $(WASM)

build: $(WASM)

clean:
	rm -rf $(DIST_DIR) $(SRC_DIR)/wit_world $(SRC_DIR)/componentize_py_*
