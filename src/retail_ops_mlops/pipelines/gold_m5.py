from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from retail_ops_mlops.utils.config import ensure_dirs, load_config

logger = logging.getLogger(__name__)

DATASET_ID = "m5"

REQUIRED_SILVER = (
    "calendar.parquet",
    "sell_prices.parquet",
    "sales_train_validation.parquet",
)


@dataclass
class GoldFileReport:
    name: str
    status: str
    input_path: str | None
    output_path: str | None
    rows: int | None
    columns: int | None
    input_bytes: int | None
    output_bytes: int | None
    input_sha256: str | None
    output_sha256: str | None


@dataclass
class GoldReport:
    pipeline: str
    dataset_id: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    config_path: str
    silver_dir: str
    gold_dir: str
    report_path: str
    files: list[GoldFileReport]
    notes: dict[str, Any]


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_parquet(table: pa.Table, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_path, compression="snappy")


def _calendar_gold(in_path: Path) -> pa.Table:
    tbl = pq.read_table(in_path)

    weekend_set = pa.array(["Saturday", "Sunday"], type=pa.string())
    is_weekend = pc.is_in(tbl["weekday"], value_set=weekend_set)
    is_event_day = pc.invert(pc.is_null(tbl["event_name_1"]))

    tbl = tbl.append_column("is_weekend", is_weekend)
    tbl = tbl.append_column("is_event_day", is_event_day)
    return tbl


def _series_dim_from_sales_validation(in_path: Path) -> pa.Table:
    cols = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]
    tbl = pq.read_table(in_path, columns=cols)

    # Small table (~30k rows) => pandas is fine for uniqueness
    df = tbl.to_pandas().drop_duplicates()
    return pa.Table.from_pandas(df, preserve_index=False)


def _sorted_day_cols(schema_names: list[str]) -> list[str]:
    day_cols = [n for n in schema_names if n.startswith("d_")]
    day_cols.sort(key=lambda s: int(s.split("_")[1]))
    return day_cols


def _sales_long_sample(
    sales_validation_path: Path,
    *,
    sample_n_series: int,
    sample_days: int,
) -> pa.Table:
    meta_cols = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]

    schema = pq.read_schema(sales_validation_path)
    day_cols_all = _sorted_day_cols(schema.names)

    if not day_cols_all:
        raise ValueError("No day columns (d_*) found in sales_train_validation.parquet")

    day_cols = day_cols_all[-min(sample_days, len(day_cols_all)) :]

    pf = pq.ParquetFile(sales_validation_path)
    cols = meta_cols + day_cols

    batches: list[pa.RecordBatch] = []
    seen = 0
    for batch in pf.iter_batches(batch_size=max(1024, sample_n_series), columns=cols):
        batches.append(batch)
        seen += batch.num_rows
        if seen >= sample_n_series:
            break

    wide = pa.Table.from_batches(batches).slice(0, sample_n_series)

    n_series = wide.num_rows
    n_days = len(day_cols)

    out: dict[str, Any] = {}
    for c in meta_cols:
        out[c] = np.repeat(wide[c].to_pylist(), n_days)

    out["d"] = np.tile(day_cols, n_series)

    day_matrix = np.stack(
        [wide[c].to_numpy(zero_copy_only=False) for c in day_cols],
        axis=1,
    )
    out["sales"] = day_matrix.reshape(-1).astype(np.int32, copy=False)

    return pa.Table.from_pydict(out)


def run(
    config_path: str | Path = "configs/default.yaml",
    *,
    force: bool = False,
    strict: bool = True,
    sample_n_series: int = 20,
    sample_days: int = 365,
) -> Path:
    """
    Gold layer for M5 (from Silver):
    - dim_calendar.parquet (calendar + simple derived flags)
    - dim_series.parquet   (unique series metadata from sales_train_validation)
    - fact_sell_prices.parquet (typed sell prices)
    - fact_sales_long_sample.parquet (small long-format sample for EDA/debug)
    - Report: outputs/reports/gold_m5.json
    """
    cfg = load_config(config_path)
    ensure_dirs(cfg)

    started = datetime.now(timezone.utc)

    silver_dir = cfg["paths"]["data_processed"] / DATASET_ID / "silver"
    gold_dir = cfg["paths"]["data_processed"] / DATASET_ID / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    report_path = cfg["paths"]["outputs_reports"] / "gold_m5.json"

    notes: dict[str, Any] = {
        "gold_definition": "Conformed analytics tables from Silver (business-friendly).",
        "includes_sales_long_sample": True,
        "sample_n_series": sample_n_series,
        "sample_days": sample_days,
        "force_overwrite": force,
    }

    files: list[GoldFileReport] = []
    status = "ok"

    if not silver_dir.exists():
        status = "missing_input"
        notes["missing_reason"] = "Silver dir not found. Run silver-m5 first."
        finished = datetime.now(timezone.utc)

        report = GoldReport(
            pipeline="gold_m5",
            dataset_id=DATASET_ID,
            status=status,
            started_at_utc=started.isoformat(),
            finished_at_utc=finished.isoformat(),
            config_path=str(cfg["config_path"]),
            silver_dir=str(silver_dir),
            gold_dir=str(gold_dir),
            report_path=str(report_path),
            files=files,
            notes=notes,
        )
        report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")
        if strict:
            raise FileNotFoundError(notes["missing_reason"])
        return report_path

    # Check required inputs
    missing = [name for name in REQUIRED_SILVER if not (silver_dir / name).exists()]
    if missing:
        status = "missing_input"
        notes["missing_files"] = missing
        notes["missing_reason"] = "Some required Silver files are missing."
        finished = datetime.now(timezone.utc)

        report = GoldReport(
            pipeline="gold_m5",
            dataset_id=DATASET_ID,
            status=status,
            started_at_utc=started.isoformat(),
            finished_at_utc=finished.isoformat(),
            config_path=str(cfg["config_path"]),
            silver_dir=str(silver_dir),
            gold_dir=str(gold_dir),
            report_path=str(report_path),
            files=files,
            notes=notes,
        )
        report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")
        if strict:
            raise FileNotFoundError(notes["missing_reason"])
        return report_path

    # 1) dim_calendar
    in_cal = silver_dir / "calendar.parquet"
    out_cal = gold_dir / "dim_calendar.parquet"

    if out_cal.exists() and not force:
        cal_rows = pq.read_metadata(out_cal).num_rows
        cal_cols = len(pq.read_schema(out_cal).names)
        files.append(
            GoldFileReport(
                name=out_cal.name,
                status="exists",
                input_path=str(in_cal),
                output_path=str(out_cal),
                rows=cal_rows,
                columns=cal_cols,
                input_bytes=in_cal.stat().st_size,
                output_bytes=out_cal.stat().st_size,
                input_sha256=sha256_file(in_cal),
                output_sha256=sha256_file(out_cal),
            )
        )
    else:
        logger.info("Gold: %s -> %s", in_cal.name, out_cal.name)
        cal = _calendar_gold(in_cal)
        _write_parquet(cal, out_cal)
        files.append(
            GoldFileReport(
                name=out_cal.name,
                status="ok",
                input_path=str(in_cal),
                output_path=str(out_cal),
                rows=cal.num_rows,
                columns=cal.num_columns,
                input_bytes=in_cal.stat().st_size,
                output_bytes=out_cal.stat().st_size,
                input_sha256=sha256_file(in_cal),
                output_sha256=sha256_file(out_cal),
            )
        )

    # 2) dim_series
    in_sales = silver_dir / "sales_train_validation.parquet"
    out_series = gold_dir / "dim_series.parquet"

    if out_series.exists() and not force:
        s_rows = pq.read_metadata(out_series).num_rows
        s_cols = len(pq.read_schema(out_series).names)
        files.append(
            GoldFileReport(
                name=out_series.name,
                status="exists",
                input_path=str(in_sales),
                output_path=str(out_series),
                rows=s_rows,
                columns=s_cols,
                input_bytes=in_sales.stat().st_size,
                output_bytes=out_series.stat().st_size,
                input_sha256=sha256_file(in_sales),
                output_sha256=sha256_file(out_series),
            )
        )
    else:
        logger.info("Gold: %s -> %s", in_sales.name, out_series.name)
        series = _series_dim_from_sales_validation(in_sales)
        _write_parquet(series, out_series)
        files.append(
            GoldFileReport(
                name=out_series.name,
                status="ok",
                input_path=str(in_sales),
                output_path=str(out_series),
                rows=series.num_rows,
                columns=series.num_columns,
                input_bytes=in_sales.stat().st_size,
                output_bytes=out_series.stat().st_size,
                input_sha256=sha256_file(in_sales),
                output_sha256=sha256_file(out_series),
            )
        )

    # 3) fact_sell_prices (copied/kept typed)
    in_prices = silver_dir / "sell_prices.parquet"
    out_prices = gold_dir / "fact_sell_prices.parquet"

    if out_prices.exists() and not force:
        p_rows = pq.read_metadata(out_prices).num_rows
        p_cols = len(pq.read_schema(out_prices).names)
        files.append(
            GoldFileReport(
                name=out_prices.name,
                status="exists",
                input_path=str(in_prices),
                output_path=str(out_prices),
                rows=p_rows,
                columns=p_cols,
                input_bytes=in_prices.stat().st_size,
                output_bytes=out_prices.stat().st_size,
                input_sha256=sha256_file(in_prices),
                output_sha256=sha256_file(out_prices),
            )
        )
    else:
        logger.info("Gold: %s -> %s", in_prices.name, out_prices.name)
        prices = pq.read_table(in_prices)
        _write_parquet(prices, out_prices)
        files.append(
            GoldFileReport(
                name=out_prices.name,
                status="ok",
                input_path=str(in_prices),
                output_path=str(out_prices),
                rows=prices.num_rows,
                columns=prices.num_columns,
                input_bytes=in_prices.stat().st_size,
                output_bytes=out_prices.stat().st_size,
                input_sha256=sha256_file(in_prices),
                output_sha256=sha256_file(out_prices),
            )
        )

    # 4) small long-format sample (for EDA/debug)
    out_sample = gold_dir / "fact_sales_long_sample.parquet"
    if out_sample.exists() and not force:
        sm_rows = pq.read_metadata(out_sample).num_rows
        sm_cols = len(pq.read_schema(out_sample).names)
        files.append(
            GoldFileReport(
                name=out_sample.name,
                status="exists",
                input_path=str(in_sales),
                output_path=str(out_sample),
                rows=sm_rows,
                columns=sm_cols,
                input_bytes=in_sales.stat().st_size,
                output_bytes=out_sample.stat().st_size,
                input_sha256=sha256_file(in_sales),
                output_sha256=sha256_file(out_sample),
            )
        )
    else:
        logger.info("Gold: building sales long sample -> %s", out_sample.name)
        sample = _sales_long_sample(
            in_sales,
            sample_n_series=sample_n_series,
            sample_days=sample_days,
        )
        _write_parquet(sample, out_sample)
        files.append(
            GoldFileReport(
                name=out_sample.name,
                status="ok",
                input_path=str(in_sales),
                output_path=str(out_sample),
                rows=sample.num_rows,
                columns=sample.num_columns,
                input_bytes=in_sales.stat().st_size,
                output_bytes=out_sample.stat().st_size,
                input_sha256=sha256_file(in_sales),
                output_sha256=sha256_file(out_sample),
            )
        )

    finished = datetime.now(timezone.utc)

    report = GoldReport(
        pipeline="gold_m5",
        dataset_id=DATASET_ID,
        status=status,
        started_at_utc=started.isoformat(),
        finished_at_utc=finished.isoformat(),
        config_path=str(cfg["config_path"]),
        silver_dir=str(silver_dir),
        gold_dir=str(gold_dir),
        report_path=str(report_path),
        files=files,
        notes=notes,
    )

    report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")

    if status != "ok" and strict:
        raise FileNotFoundError(notes.get("missing_reason", "Gold pipeline failed."))

    return report_path
