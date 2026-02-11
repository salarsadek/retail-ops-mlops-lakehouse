from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from retail_ops_mlops.utils.config import ensure_dirs, load_config

logger = logging.getLogger(__name__)

DATASET_ID = "m5"

BRONZE_FILES = (
    "calendar.parquet",
    "sell_prices.parquet",
    "sales_train_validation.parquet",
    "sales_train_evaluation.parquet",
    "sample_submission.parquet",
)


@dataclass
class SilverFileReport:
    name: str
    status: str
    input_path: str
    output_path: str | None
    rows: int | None
    columns: int | None
    input_bytes: int | None
    output_bytes: int | None
    input_sha256: str | None
    output_sha256: str | None


@dataclass
class SilverReport:
    pipeline: str
    dataset_id: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    config_path: str
    bronze_dir: str
    silver_dir: str
    report_path: str
    files: list[SilverFileReport]
    notes: dict[str, Any]


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _set(table: pa.Table, name: str, arr: pa.Array | pa.ChunkedArray) -> pa.Table:
    if name not in table.column_names:
        return table
    idx = table.schema.get_field_index(name)
    return table.set_column(idx, name, arr)


def _cast(table: pa.Table, name: str, dtype: pa.DataType) -> pa.Table:
    if name not in table.column_names:
        return table
    idx = table.schema.get_field_index(name)
    return table.set_column(idx, name, pc.cast(table[name], dtype))


def _parse_date32(col: pa.Array | pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
    if pa.types.is_date32(col.type):
        return col
    if pa.types.is_timestamp(col.type):
        return pc.cast(col, pa.date32())
    if pa.types.is_string(col.type):
        ts = pc.strptime(col, format="%Y-%m-%d", unit="s", error_is_null=True)
        return pc.cast(ts, pa.date32())
    return pc.cast(col, pa.date32())


def _process_calendar(t: pa.Table) -> pa.Table:
    t = _set(t, "date", _parse_date32(t["date"]))
    t = _cast(t, "wm_yr_wk", pa.int32())
    for c in ("wday", "month", "year"):
        t = _cast(t, c, pa.int16())
    for c in ("snap_CA", "snap_TX", "snap_WI"):
        t = _cast(t, c, pa.int8())
    return t


def _process_sell_prices(t: pa.Table) -> pa.Table:
    t = _cast(t, "wm_yr_wk", pa.int32())
    t = _cast(t, "sell_price", pa.float32())
    return t


def _process_sales_wide(t: pa.Table) -> pa.Table:
    for name in t.column_names:
        if name.startswith("d_"):
            t = _cast(t, name, pa.int32())
    return t


def _process_sample_submission(t: pa.Table) -> pa.Table:
    for name in t.column_names:
        if name.startswith("F"):
            t = _cast(t, name, pa.float32())
    return t


_PROCESSORS: dict[str, Any] = {
    "calendar.parquet": _process_calendar,
    "sell_prices.parquet": _process_sell_prices,
    "sales_train_validation.parquet": _process_sales_wide,
    "sales_train_evaluation.parquet": _process_sales_wide,
    "sample_submission.parquet": _process_sample_submission,
}


def run(
    config_path: str | Path = "configs/default.yaml",
    *,
    force: bool = False,
    strict: bool = True,
) -> Path:
    """
    Silver layer for M5 (typed/cleaned parquet copies).

    Input:  data/interim/m5/bronze/*.parquet
    Output: data/processed/m5/silver/*.parquet
    Report: outputs/reports/silver_m5.json
    """
    cfg = load_config(config_path)
    ensure_dirs(cfg)

    started = datetime.now(timezone.utc)

    bronze_dir = cfg["paths"]["data_interim"] / DATASET_ID / "bronze"
    silver_dir = cfg["paths"]["data_processed"] / DATASET_ID / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    report_path = cfg["paths"]["outputs_reports"] / "silver_m5.json"

    notes: dict[str, Any] = {
        "silver_definition": "Typed/structured Parquet. No feature engineering yet.",
        "input_bronze_dir": str(bronze_dir),
        "output_silver_dir": str(silver_dir),
        "force_overwrite": force,
    }

    file_reports: list[SilverFileReport] = []
    overall_status = "ok"

    if not bronze_dir.exists():
        overall_status = "missing_input"
        notes["missing_reason"] = "Bronze dir not found. Run bronze-m5 first."

    for name in BRONZE_FILES:
        in_path = bronze_dir / name
        out_path = silver_dir / name

        if not in_path.exists():
            overall_status = "partial" if overall_status == "ok" else overall_status
            file_reports.append(
                SilverFileReport(
                    name=name,
                    status="missing",
                    input_path=str(in_path),
                    output_path=None,
                    rows=None,
                    columns=None,
                    input_bytes=None,
                    output_bytes=None,
                    input_sha256=None,
                    output_sha256=None,
                )
            )
            continue

        if out_path.exists() and not force:
            meta = pq.read_metadata(out_path)
            file_reports.append(
                SilverFileReport(
                    name=name,
                    status="exists",
                    input_path=str(in_path),
                    output_path=str(out_path),
                    rows=meta.num_rows,
                    columns=meta.num_columns,
                    input_bytes=in_path.stat().st_size,
                    output_bytes=out_path.stat().st_size,
                    input_sha256=sha256_file(in_path),
                    output_sha256=sha256_file(out_path),
                )
            )
            continue

        try:
            logger.info("Silver: %s -> %s", in_path.name, out_path.name)
            t = pq.read_table(in_path)
            t2 = _PROCESSORS.get(name, lambda x: x)(t)
            pq.write_table(t2, out_path, compression="snappy", use_dictionary=True)

            meta = pq.read_metadata(out_path)
            file_reports.append(
                SilverFileReport(
                    name=name,
                    status="ok",
                    input_path=str(in_path),
                    output_path=str(out_path),
                    rows=meta.num_rows,
                    columns=meta.num_columns,
                    input_bytes=in_path.stat().st_size,
                    output_bytes=out_path.stat().st_size,
                    input_sha256=sha256_file(in_path),
                    output_sha256=sha256_file(out_path),
                )
            )
        except Exception as err:
            overall_status = "error"
            notes["error"] = repr(err)
            file_reports.append(
                SilverFileReport(
                    name=name,
                    status="error",
                    input_path=str(in_path),
                    output_path=str(out_path) if out_path.exists() else None,
                    rows=None,
                    columns=None,
                    input_bytes=in_path.stat().st_size if in_path.exists() else None,
                    output_bytes=out_path.stat().st_size if out_path.exists() else None,
                    input_sha256=sha256_file(in_path) if in_path.exists() else None,
                    output_sha256=sha256_file(out_path) if out_path.exists() else None,
                )
            )

    finished = datetime.now(timezone.utc)

    report = SilverReport(
        pipeline="silver_m5",
        dataset_id=DATASET_ID,
        status=overall_status,
        started_at_utc=started.isoformat(),
        finished_at_utc=finished.isoformat(),
        config_path=str(cfg["config_path"]),
        bronze_dir=str(bronze_dir),
        silver_dir=str(silver_dir),
        report_path=str(report_path),
        files=file_reports,
        notes=notes,
    )

    report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")

    if overall_status != "ok" and strict:
        raise RuntimeError(notes.get("missing_reason", "Silver pipeline failed."))

    return report_path
