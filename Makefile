reset-python:
	pre-commit clean
	rm -rf .venv
.PHONY: reset-python

install-dev:
	cd py && \
	pip install -r requirements.txt && \
	pip install -r requirements-dev.txt && \
	pip install -e .
.PHONY: install-dev

install-pre-commit-hook:
	pre-commit install --install-hooks
.PHONY: install-pre-commit-hook

tests:
	cd py/tests && pytest -vv .
.PHONY: tests
