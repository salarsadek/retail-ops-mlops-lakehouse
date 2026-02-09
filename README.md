@'
# Retail Ops ML (Lakehouse + MLOps) — Capstone

End-to-end demo: ingest → validate → transform (Spark) → dbt tests → train (MLflow) → score → monitor → orchestrate (Airflow).

## Dev setup
```bash
python -m venv .venv
source .venv/bin/activate  # Linux/mac
# Windows PowerShell: .\.venv\Scripts\Activate.ps1

pip install -U pip
pip install -e ".[dev]"
pre-commit install

ruff format .
ruff check .
pytest
