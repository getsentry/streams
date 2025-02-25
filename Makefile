install-dev:
	./scripts/flink-jar-download.sh
	which uv || (curl -LsSf https://astral.sh/uv/install.sh | sh)
	uv sync --project ./sentry_streams
.PHONY: install-dev

install-pre-commit-hook:
	./sentry_streams/.venv/bin/pre-commit install --install-hooks
.PHONY: install-pre-commit-hook

tests:
	./sentry_streams/.venv/bin/pytest -vv sentry_streams/tests
.PHONY: tests

typecheck:
	./sentry_streams/.venv/bin/mypy --config-file sentry_streams/mypy.ini --strict sentry_streams/
.PHONY: typecheck
