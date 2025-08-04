reset:
	rm -rf sentry_streams/.venv
	cd sentry_streams && cargo clean
.PHONY: reset

install-dev:
	./scripts/flink-jar-download.sh
	which uv || (curl -LsSf https://astral.sh/uv/0.7.13/install.sh | sh)
	uv sync --project ./sentry_streams
	PROJECT_ROOT=`pwd`/sentry_flink uv sync --project ./sentry_flink
.PHONY: install-dev

install-pre-commit-hook:
	./sentry_streams/.venv/bin/pre-commit install --install-hooks
.PHONY: install-pre-commit-hook

tests-streams:
	./sentry_streams/.venv/bin/pytest -vv sentry_streams/tests
.PHONY: tests-streams

tests-integration:
	./sentry_streams/.venv/bin/pytest -vv sentry_streams/integration_tests
.PHONY: tests-integration

test-rust-streams: tests-rust-streams
.PHONY: test-rust-streams

tests-rust-streams:
	. sentry_streams/.venv/bin/activate && . scripts/rust-envvars && cd ./sentry_streams/ && cargo test
.PHONY: tests-rust-streams

tests-flink:
	./sentry_flink/.venv/bin/pytest -vv sentry_flink/tests
.PHONY: tests-flink

typecheck:
	. ./sentry_streams/.venv/bin/activate && cd ./sentry_streams/sentry_streams/examples/rust_simple_map_filter/rust_transforms/ && maturin develop
	./sentry_streams/.venv/bin/mypy --config-file sentry_streams/mypy.ini --strict sentry_streams/
	./sentry_flink/.venv/bin/mypy --config-file sentry_flink/mypy.ini --strict sentry_flink/sentry_flink/
.PHONY: typecheck

build-streams:
	cd sentry_streams && uv pip install wheel
	cd sentry_streams && uv pip install build
	cd sentry_streams && .venv/bin/python -m build --wheel
.PHONY: build-streams

docs:
	uv sync --project ./sentry_streams --group docs
	./sentry_streams/.venv/bin/sphinx-build -b html sentry_streams/docs/source/ sentry_streams/docs/build/
.PHONY: docs
