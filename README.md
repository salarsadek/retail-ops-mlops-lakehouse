# retail-ops-mlops-lakehouse

PowerShell-robust, cross-platform baseline pipeline for M5-style retail forecasting with:
- Data quality checks
- Training + evaluation artifacts (tables/figures/reports)
- Reproducible local + CI execution

## Quickstart (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -e ".[dev]"
python -m pre_commit install

# Run end-to-end pipeline
.\scripts\run_m5.ps1 -Force
```

Outputs are written to `outputs/` and sample data is created under `data/processed/m5/gold/`.

## Quickstart (Linux/macOS)

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install -e ".[dev]"
python -m pre_commit install

pwsh ./scripts/run_m5.ps1 -Force
```

> Requires PowerShell (`pwsh`) on Linux/macOS.

## Commands

- Ensure folders:
  - `python -m retail_ops_mlops.cli ensure-dirs`
- Train baseline:
  - `python -m retail_ops_mlops.cli train-m5 --force`
- Evaluate baseline:
  - `python -m retail_ops_mlops.cli eval-m5 --force`

## Repo structure (high level)

- `src/retail_ops_mlops/` – library + CLI + pipelines
- `scripts/run_m5.ps1` – end-to-end runner (cross-platform)
- `tests/` – smoke test for pipeline runner
- `outputs/` – generated artifacts (ignored by git)
- `data/` – raw/interim/processed (raw/processed contents ignored by git)

## CI

GitHub Actions runs:
- lint/format via pre-commit
- pytest (ubuntu + windows)

## License

MIT (or update as needed).
