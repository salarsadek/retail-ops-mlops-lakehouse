SHELL := /bin/bash

.PHONY: help lint test m5 m5-dq show-paths ensure-dirs

help:
	@echo "Targets:"
	@echo "  make lint        - format + lint + pre-commit"
	@echo "  make test        - run pytest"
	@echo "  make m5          - run full M5 pipeline (run-m5)"
	@echo "  make m5-dq       - run DQ checks on Gold (dq-m5)"
	@echo "  make show-paths  - show resolved paths"
	@echo "  make ensure-dirs - create required directories"

lint:
	ruff format src
	ruff check src
	pre-commit run -a

test:
	pytest -q

show-paths:
	python -m retail_ops_mlops show-paths

ensure-dirs:
	python -m retail_ops_mlops ensure-dirs

m5:
	python -m retail_ops_mlops run-m5

m5-dq:
	python -m retail_ops_mlops dq-m5
