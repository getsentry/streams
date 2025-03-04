install-dev:
	./scripts/flink-jar-download.sh
	which uv || (curl -LsSf https://astral.sh/uv/install.sh | sh)
	uv sync --project ./sentry_streams
	PROJECT_ROOT=`pwd`/sentry_flink uv sync --project ./sentry_flink
.PHONY: install-dev

install-pre-commit-hook:
	./sentry_streams/.venv/bin/pre-commit install --install-hooks
.PHONY: install-pre-commit-hook

tests-streams:
	./sentry_streams/.venv/bin/pytest -vv sentry_streams/tests
.PHONY: tests-streams

tests-flink:
	./sentry_flink/.venv/bin/pytest -vv sentry_flink/tests
.PHONY: tests-flink

typecheck:
	./sentry_streams/.venv/bin/mypy --config-file sentry_streams/mypy.ini --strict sentry_streams/sentry_streams/
	./sentry_flink/.venv/bin/mypy --config-file sentry_flink/mypy.ini --strict sentry_flink/sentry_flink/
.PHONY: typecheck

build-streams:
	uv pip install wheel
	uv pip install build
	cd sentry_streams && .venv/bin/python -m build --wheel
.PHONY: build-streams
