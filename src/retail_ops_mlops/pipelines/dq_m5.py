from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from retail_ops_mlops.utils.config import load_cfg


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fail(msg: str) -> None:
    raise ValueError(msg)


def run(config_path: Path, horizon: int = 28, force: bool = False) -> Path:
    cfg = load_cfg(config_path)

    gold_dir = Path(cfg["paths"]["data_processed"]) / "m5" / "gold"
    outputs_reports = Path(cfg["paths"]["outputs_reports"])
    outputs_reports.mkdir(parents=True, exist_ok=True)

    features_path = gold_dir / "fact_sales_features_sample.parquet"
    if not features_path.exists():
        raise FileNotFoundError(
            f"Missing features table: {features_path}. Run build-features-m5 first."
        )

    report_path = outputs_reports / "dq_m5.json"
    if report_path.exists() and (not force):
        raise FileExistsError(f"Exists: {report_path}. Use --force to overwrite.")

    started = _utc_now()
    df = pd.read_parquet(features_path)

    # --- required columns
    req = {"id", "d", "sales", "d_num", "is_test"}
    missing = req - set(df.columns)
    if missing:
        _fail(f"Missing required columns in {features_path}: {sorted(missing)}")

    # --- basic sanity
    if df.empty:
        _fail("DQ fail: features table is empty.")

    # --- is_test split size check: expected = n_series * horizon
    h = int(horizon)
    n_series = int(df["id"].nunique())
    n_test = int(df["is_test"].sum())
    expected_test = n_series * h

    if n_test != expected_test:
        _fail(
            "Unexpected test size: "
            f"n_test={n_test}, expected={expected_test} "
            f"(= n_series {n_series} * horizon {h})"
        )

    # --- engineered feature NaNs should be 0 in TEST
    feat_cols = ["lag_1", "lag_7", "lag_28", "roll_mean_7", "roll_mean_28"]
    for c in feat_cols:
        if c not in df.columns:
            _fail(f"Missing engineered feature column '{c}' in features table.")

    test_na = df.loc[df["is_test"], feat_cols].isna().sum().to_dict()
    if any(int(v) > 0 for v in test_na.values()):
        _fail(f"NaNs found in TEST engineered features: {test_na}")

    # --- report
    report: dict[str, Any] = {
        "pipeline": "dq_m5",
        "dataset_id": "m5",
        "status": "ok",
        "started_at_utc": started,
        "finished_at_utc": _utc_now(),
        "config_path": str(config_path),
        "features_path": str(features_path),
        "n_rows": int(df.shape[0]),
        "n_series": n_series,
        "horizon": h,
        "n_test": n_test,
        "expected_test": expected_test,
        "test_na_counts": {k: int(v) for k, v in test_na.items()},
        "notes": {"force_overwrite": bool(force)},
    }

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"OK: wrote report: {report_path}")
    return report_path
