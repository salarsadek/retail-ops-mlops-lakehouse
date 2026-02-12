from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from retail_ops_mlops.utils.config import load_cfg


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(config_path: Path, horizon: int = 28, force: bool = False) -> Path:
    cfg = load_cfg(config_path)
    paths = cfg.get("paths", {})

    root = Path.cwd()
    data_processed = (root / paths["data_processed"]).resolve()
    outputs_reports = (root / paths["outputs_reports"]).resolve()
    outputs_reports.mkdir(parents=True, exist_ok=True)

    gold_dir = data_processed / "m5" / "gold"
    sales_path = gold_dir / "fact_sales_long_sample.parquet"
    cal_path = gold_dir / "dim_calendar.parquet"

    if not sales_path.exists():
        raise FileNotFoundError(f"Missing sales sample: {sales_path}")
    if not cal_path.exists():
        raise FileNotFoundError(f"Missing calendar: {cal_path}")

    out_path = gold_dir / "fact_sales_features_sample.parquet"
    report_path = outputs_reports / "build_features_m5.json"

    if out_path.exists() and not force:
        raise FileExistsError(f"Exists: {out_path}. Use --force to overwrite.")
    if report_path.exists() and not force:
        raise FileExistsError(f"Exists: {report_path}. Use --force to overwrite.")

    started = _utc_now()

    df = pd.read_parquet(sales_path)
    cal = pd.read_parquet(cal_path)

    # Basic schema checks
    need_sales = {"id", "d", "sales"}
    missing_sales = need_sales - set(df.columns)
    if missing_sales:
        raise ValueError(f"Missing cols in {sales_path}: {sorted(missing_sales)}")

    # d_num for ordering
    df["d_num"] = df["d"].astype(str).str.replace("d_", "", regex=False).astype(int)

    # Sort + lags/rolling per id
    df = df.sort_values(["id", "d_num"]).reset_index(drop=True)
    g = df.groupby("id")["sales"]

    df["lag_1"] = g.shift(1)
    df["lag_7"] = g.shift(7)
    df["lag_28"] = g.shift(28)
    df["roll_mean_7"] = g.shift(1).rolling(7).mean().reset_index(level=0, drop=True)
    df["roll_mean_28"] = g.shift(1).rolling(28).mean().reset_index(level=0, drop=True)

    # Mark last horizon days per id as test
    h = int(horizon)
    df["is_test"] = df.groupby("id")["d_num"].transform(lambda s: s >= (s.max() - (h - 1)))

    # Calendar join for time features + SNAP flags (if present)
    # dim_calendar in M5 has "d" key; keep only columns we need if they exist
    cal_cols = ["d", "wm_yr_wk", "wday", "month", "year", "snap_CA", "snap_TX", "snap_WI"]
    cal_keep = [c for c in cal_cols if c in cal.columns]
    if "d" not in cal_keep:
        raise ValueError(f"Calendar table must include 'd' column: {cal_path}")
    cal2 = cal[cal_keep].copy()

    df = df.merge(cal2, on="d", how="left")

    # If SNAP cols missing, create as 0 for stability
    for c in ["snap_CA", "snap_TX", "snap_WI"]:
        if c not in df.columns:
            df[c] = 0

    # Ensure time cols exist
    for c in ["wm_yr_wk", "wday", "month", "year"]:
        if c not in df.columns:
            raise ValueError(f"Missing '{c}' after calendar join. Check {cal_path} columns.")

    # Save
    df.to_parquet(out_path, index=False)

    report: dict[str, Any] = {
        "pipeline": "build_features_m5",
        "dataset_id": "m5",
        "status": "ok",
        "started_at_utc": started,
        "finished_at_utc": _utc_now(),
        "config_path": str(config_path),
        "sales_path": str(sales_path),
        "calendar_path": str(cal_path),
        "features_path": str(out_path),
        "n_rows": int(df.shape[0]),
        "n_test": int(df["is_test"].sum()),
        "horizon": int(horizon),
        "notes": {"force_overwrite": bool(force), "cwd_root": str(root)},
    }

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"OK: wrote features: {out_path}")
    print(f"OK: wrote report: {report_path}")
    return report_path
