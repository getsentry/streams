install-dev:
	./scripts/flink-jar-download.sh
	which uv || (curl -LsSf https://astral.sh/uv/install.sh | sh)
	uv sync --project ./py
.PHONY: install-dev

install-pre-commit-hook:
	./py/.venv/bin/pre-commit install --install-hooks
.PHONY: install-pre-commit-hook

tests:
	./py/.venv/bin/pytest -vv py/tests
.PHONY: tests

typecheck:
	./py/.venv/bin/mypy --config-file py/mypy.ini --strict py/
.PHONY: typecheck
