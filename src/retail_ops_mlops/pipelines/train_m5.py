from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from retail_ops_mlops.utils.config import load_cfg


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(config_path: Path, horizon: int = 28, force: bool = False) -> Path:
    cfg = load_cfg(config_path)
    paths = cfg.get("paths", {})

    # Always resolve paths relative to current working directory
    # (robust when running from PowerShell in repo root)
    root = Path.cwd()
    data_processed = (root / paths["data_processed"]).resolve()
    outputs_models = (root / paths["outputs_models"]).resolve()
    outputs_reports = (root / paths["outputs_reports"]).resolve()

    outputs_models.mkdir(parents=True, exist_ok=True)
    outputs_reports.mkdir(parents=True, exist_ok=True)

    gold_dir = data_processed / "m5" / "gold"
    features_path = gold_dir / "fact_sales_features_sample.parquet"
    if not features_path.exists():
        raise FileNotFoundError(
            f"Missing features table: {features_path}. "
            "Create it first (build-features step) before training."
        )

    started = _utc_now()
    df = pd.read_parquet(features_path)

    req = {"id", "d", "sales", "d_num", "is_test"}
    missing = req - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in {features_path}: {sorted(missing)}")

    feat_cols_cat = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]
    feat_cols_num = [
        "lag_1",
        "lag_7",
        "lag_28",
        "roll_mean_7",
        "roll_mean_28",
        "d_num",
        "wm_yr_wk",
        "wday",
        "month",
        "year",
        "snap_CA",
        "snap_TX",
        "snap_WI",
    ]
    feat_cols = feat_cols_num + feat_cols_cat

    for c in feat_cols:
        if c not in df.columns:
            raise ValueError(f"Missing feature column '{c}' in {features_path}.")

    train_df = df[~df["is_test"]].copy()
    train_df = train_df.dropna(subset=["lag_1", "lag_7", "lag_28", "roll_mean_7", "roll_mean_28"])
    if train_df.empty:
        raise ValueError("Train set became empty after dropping NaNs. Check feature builder.")

    X_train = train_df[feat_cols]
    y_train = train_df["sales"].astype(float)

    pre = ColumnTransformer(
        transformers=[
            ("num", "passthrough", feat_cols_num),
            ("cat", OneHotEncoder(handle_unknown="ignore"), feat_cols_cat),
        ]
    )

    pipe = Pipeline(steps=[("pre", pre), ("model", Ridge())])
    pipe.fit(X_train, y_train)

    model_path = outputs_models / "m5_ridge_baseline.joblib"
    report_path = outputs_reports / "train_m5.json"

    if model_path.exists() and not force:
        raise FileExistsError(f"Model exists: {model_path}. Use --force to overwrite.")
    if report_path.exists() and not force:
        raise FileExistsError(f"Report exists: {report_path}. Use --force to overwrite.")

    joblib.dump(pipe, model_path)

    report: dict[str, Any] = {
        "pipeline": "train_m5",
        "dataset_id": "m5",
        "status": "ok",
        "started_at_utc": started,
        "finished_at_utc": _utc_now(),
        "config_path": str(config_path),
        "features_path": str(features_path),
        "model_path": str(model_path),
        "n_rows": int(df.shape[0]),
        "n_train_used": int(train_df.shape[0]),
        "horizon": int(horizon),
        "notes": {"force_overwrite": bool(force), "cwd_root": str(root)},
    }

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"OK: wrote report: {report_path}")
    return report_path
