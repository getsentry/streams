install-dev:
	which uv || (curl -LsSf https://astral.sh/uv/install.sh | sh)
	uv sync
.PHONY: install-dev

install-pre-commit-hook:
	.venv/bin/pre-commit install --install-hooks
.PHONY: install-pre-commit-hook

tests:
	.venv/bin/pytest -vv py/tests
.PHONY: tests

typecheck:
	.venv/bin/mypy --config-file py/mypy.ini --strict py/
.PHONY: typecheck
