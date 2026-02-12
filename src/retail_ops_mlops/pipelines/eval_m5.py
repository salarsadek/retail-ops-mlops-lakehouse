from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from retail_ops_mlops.utils.config import load_cfg


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _rmse(y_true, y_pred) -> float:
    # Robust across sklearn versions: avoid squared=False incompatibilities
    return float(mean_squared_error(y_true, y_pred) ** 0.5)


def _write_metrics_table(metrics: dict[str, float], csv_path: Path, tex_path: Path) -> None:
    rows = [{"metric": k.upper(), "value": float(v)} for k, v in metrics.items()]
    dfm = pd.DataFrame(rows)
    dfm.to_csv(csv_path, index=False)

    lines = []
    lines.append(r"\begin{tabular}{lr}")
    lines.append(r"\hline")
    lines.append(r"Metric & Value \\")
    lines.append(r"\hline")
    for r in rows:
        lines.append(f"{r['metric']} & {r['value']:.6g} \\\\")
    lines.append(r"\hline")
    lines.append(r"\end{tabular}")
    tex_path.write_text("\n".join(lines), encoding="utf-8")


def run(config_path: Path, horizon: int = 28, force: bool = False) -> Path:
    cfg = load_cfg(config_path)
    paths = cfg.get("paths", {})

    root = Path.cwd()
    data_processed = (root / paths["data_processed"]).resolve()
    outputs_figures = (root / paths["outputs_figures"]).resolve()
    outputs_tables = (root / paths["outputs_tables"]).resolve()
    outputs_models = (root / paths["outputs_models"]).resolve()
    outputs_reports = (root / paths["outputs_reports"]).resolve()

    outputs_figures.mkdir(parents=True, exist_ok=True)
    outputs_tables.mkdir(parents=True, exist_ok=True)
    outputs_models.mkdir(parents=True, exist_ok=True)
    outputs_reports.mkdir(parents=True, exist_ok=True)

    gold_dir = data_processed / "m5" / "gold"
    features_path = gold_dir / "fact_sales_features_sample.parquet"
    if not features_path.exists():
        raise FileNotFoundError(f"Missing features table: {features_path}. Create it first.")

    model_path = outputs_models / "m5_ridge_baseline.joblib"
    if not model_path.exists():
        raise FileNotFoundError(f"Missing model: {model_path}. Run train-m5 first.")

    report_path = outputs_reports / "eval_m5.json"
    metrics_csv = outputs_tables / "eval_m5_metrics.csv"
    metrics_tex = outputs_tables / "eval_m5_metrics.tex"
    preds_csv = outputs_tables / "eval_m5_predictions.csv"

    fig_pred_pdf = outputs_figures / "eval_m5_pred_vs_true.pdf"
    fig_pred_png = outputs_figures / "eval_m5_pred_vs_true.png"
    fig_res_pdf = outputs_figures / "eval_m5_residuals.pdf"
    fig_res_png = outputs_figures / "eval_m5_residuals.png"

    for p in [
        report_path,
        metrics_csv,
        metrics_tex,
        preds_csv,
        fig_pred_pdf,
        fig_pred_png,
        fig_res_pdf,
        fig_res_png,
    ]:
        if p.exists() and not force:
            raise FileExistsError(f"Exists: {p}. Use --force to overwrite.")

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

    test_df = df[df["is_test"]].copy()
    test_df = test_df.dropna(subset=["lag_1", "lag_7", "lag_28", "roll_mean_7", "roll_mean_28"])
    if test_df.empty:
        raise ValueError(
            "After dropping NaNs, test_df became empty. Check feature builder + horizon."
        )

    X_test = test_df[feat_cols]
    y_true = test_df["sales"].astype(float).values

    pipe = joblib.load(model_path)
    y_pred = pipe.predict(X_test)

    metrics = {
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "rmse": _rmse(y_true, y_pred),
        "r2": float(r2_score(y_true, y_pred)),
    }

    out = test_df[["id", "d", "sales"]].copy().rename(columns={"sales": "y_true"})
    out["y_pred"] = y_pred
    out.to_csv(preds_csv, index=False)

    plt.figure()
    plt.scatter(out["y_true"], out["y_pred"], s=10)
    plt.xlabel("y_true")
    plt.ylabel("y_pred")
    plt.title("M5 baseline: Pred vs True")
    plt.tight_layout()
    plt.savefig(fig_pred_pdf)
    plt.savefig(fig_pred_png)
    plt.close()

    resid = out["y_true"] - out["y_pred"]
    plt.figure()
    plt.hist(resid, bins=50)
    plt.xlabel("residual (y_true - y_pred)")
    plt.ylabel("count")
    plt.title("M5 baseline: Residuals")
    plt.tight_layout()
    plt.savefig(fig_res_pdf)
    plt.savefig(fig_res_png)
    plt.close()

    _write_metrics_table(metrics, metrics_csv, metrics_tex)

    report: dict[str, Any] = {
        "pipeline": "eval_m5",
        "dataset_id": "m5",
        "status": "ok",
        "started_at_utc": started,
        "finished_at_utc": _utc_now(),
        "config_path": str(config_path),
        "features_path": str(features_path),
        "model_path": str(model_path),
        "report_path": str(report_path),
        "metrics_path_csv": str(metrics_csv),
        "metrics_path_tex": str(metrics_tex),
        "predictions_path_csv": str(preds_csv),
        "figures": {
            "pred_vs_true_pdf": str(fig_pred_pdf),
            "pred_vs_true_png": str(fig_pred_png),
            "residuals_pdf": str(fig_res_pdf),
            "residuals_png": str(fig_res_png),
        },
        "n_rows": int(df.shape[0]),
        "n_test": int(test_df.shape[0]),
        "horizon": int(horizon),
        "metrics": metrics,
        "notes": {"force_overwrite": bool(force), "cwd_root": str(root)},
    }

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"OK: wrote report: {report_path}")
    return report_path
