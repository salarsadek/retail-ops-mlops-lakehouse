from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pandas as pd


def _powershell_exe() -> str:
    # Windows: powershell, Linux/macOS: pwsh
    return "powershell" if os.name == "nt" else "pwsh"


def _ensure_minimal_gold(root: Path) -> None:
    """
    Create a tiny M5-like gold dataset if missing.
    This makes the pipeline + CI work in a clean checkout.
    """
    gold = root / "data" / "processed" / "m5" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    fact_sales = gold / "fact_sales_long_sample.parquet"
    dim_cal = gold / "dim_calendar.parquet"

    if fact_sales.exists() and dim_cal.exists():
        return

    # Minimal calendar: 60 days
    start_d = 1900
    n_days = 60
    d_nums = list(range(start_d, start_d + n_days))
    d = [f"d_{x}" for x in d_nums]
    cal = pd.DataFrame(
        {
            "d": d,
            "wm_yr_wk": [11600 + (i // 7) for i in range(n_days)],
            "wday": [(i % 7) + 1 for i in range(n_days)],
            "month": [1 + (i // 30) for i in range(n_days)],
            "year": [2016 for _ in range(n_days)],
            "snap_CA": [0 for _ in range(n_days)],
            "snap_TX": [0 for _ in range(n_days)],
            "snap_WI": [0 for _ in range(n_days)],
        }
    )
    cal.to_parquet(dim_cal, index=False)

    # Minimal sales: 2 series x 60 days
    ids = ["ITEM_1_CA_1", "ITEM_2_TX_1"]
    rows = []
    for sid in ids:
        item_id, state = sid.split("_")[0] + "_" + sid.split("_")[1], sid.split("_")[2]
        store_id = f"{state}_1"
        dept_id = "FOODS_1"
        cat_id = "FOODS"
        state_id = state
        for i, dd in enumerate(d):
            # simple pattern: zeros + some spikes
            sales = 0.0
            if i % 7 == 0:
                sales = 3.0
            if i % 13 == 0:
                sales += 5.0
            rows.append(
                {
                    "id": sid,
                    "item_id": item_id,
                    "dept_id": dept_id,
                    "cat_id": cat_id,
                    "store_id": store_id,
                    "state_id": state_id,
                    "d": dd,
                    "sales": float(sales),
                }
            )
    df = pd.DataFrame(rows)
    df.to_parquet(fact_sales, index=False)


def test_smoke_run_m5_ps1():
    root = Path(__file__).resolve().parents[1]
    ps1 = root / "scripts" / "run_m5.ps1"
    assert ps1.exists(), f"Missing: {ps1}"

    # Ensure minimal data exists so CI can run
    _ensure_minimal_gold(root)

    # Run pipeline (Force overwrite artifacts)
    shell = _powershell_exe()
    cmd = [
        shell,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ps1),
        "-Force",
    ]
    proc = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True)
    if proc.returncode != 0:
        print("STDOUT:\n", proc.stdout)
        print("STDERR:\n", proc.stderr)
    assert proc.returncode == 0

    # Check key artifacts exist
    required = [
        root / "data" / "processed" / "m5" / "gold" / "fact_sales_features_sample.parquet",
        root / "outputs" / "reports" / "dq_m5.json",
        root / "outputs" / "models" / "m5_ridge_baseline.joblib",
        root / "outputs" / "reports" / "eval_m5.json",
        root / "outputs" / "tables" / "eval_m5_metrics.csv",
        root / "outputs" / "figures" / "eval_m5_pred_vs_true.pdf",
    ]
    for p in required:
        assert p.exists(), f"Missing artifact: {p}"
