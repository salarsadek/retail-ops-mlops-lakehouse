from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

from retail_ops_mlops.utils.config import ensure_dirs, load_config

logger = logging.getLogger(__name__)

DATASET_ID = "m5"

GOLD_FILES: tuple[tuple[str, bool], ...] = (
    ("dim_calendar.parquet", True),
    ("dim_series.parquet", True),
    ("fact_sell_prices.parquet", True),
    ("fact_sales_long_sample.parquet", False),  # optional sample
)


@dataclass
class DQCheck:
    name: str
    status: str  # pass|warn|fail
    details: dict[str, Any]


@dataclass
class DQFileReport:
    name: str
    status: str  # ok|missing
    path: str
    rows: int | None
    columns: int | None
    bytes: int | None
    sha256: str | None
    checks: list[DQCheck]


@dataclass
class DQReport:
    pipeline: str
    dataset_id: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    config_path: str
    gold_dir: str
    report_path: str
    files: list[DQFileReport]
    notes: dict[str, Any]


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _count_nulls(path: Path, column: str, *, batch_size: int = 1_000_000) -> int:
    pf = pq.ParquetFile(path)
    nulls = 0
    for batch in pf.iter_batches(columns=[column], batch_size=batch_size):
        arr = batch.column(0)
        nulls += int(pc.sum(pc.is_null(arr)).as_py() or 0)
    return nulls


def _min_max_nulls_numeric(
    path: Path, column: str, *, batch_size: int = 1_000_000
) -> tuple[float | int | None, float | int | None, int]:
    pf = pq.ParquetFile(path)
    min_val: float | int | None = None
    max_val: float | int | None = None
    nulls = 0

    for batch in pf.iter_batches(columns=[column], batch_size=batch_size):
        arr = batch.column(0)
        nulls += int(pc.sum(pc.is_null(arr)).as_py() or 0)

        bmin = pc.min(arr).as_py()
        bmax = pc.max(arr).as_py()

        if bmin is not None:
            min_val = bmin if min_val is None else min(min_val, bmin)
        if bmax is not None:
            max_val = bmax if max_val is None else max(max_val, bmax)

    return min_val, max_val, nulls


def _distinct_count_small(path: Path, column: str) -> int:
    # Safe for small-ish dims (calendar ~2k rows, series ~30k rows)
    t = pq.read_table(path, columns=[column])
    return int(pc.count_distinct(t.column(0)).as_py() or 0)


def run(
    config_path: str | Path = "configs/default.yaml",
    *,
    strict: bool = True,
) -> Path:
    """
    Data Quality checks for M5 Gold tables.
    Writes: outputs/reports/dq_m5.json

    Goals:
    - Existence checks for expected Gold tables
    - Basic invariants: non-empty, key not-null, dim uniqueness
    - Light numeric sanity: sell_price non-negative (min/max)
    - Optional referential checks on the small sales sample (if present)
    """
    cfg = load_config(config_path)
    ensure_dirs(cfg)

    started = datetime.now(timezone.utc)

    gold_dir = cfg["paths"]["data_processed"] / DATASET_ID / "gold"
    report_path = cfg["paths"]["outputs_reports"] / "dq_m5.json"

    notes: dict[str, Any] = {
        "dq_definition": "Lightweight invariants to catch broken outputs early.",
        "gold_dir": str(gold_dir),
        "strict": strict,
    }

    file_reports: list[DQFileReport] = []
    overall = "ok"
    failures: list[str] = []

    # Load small dims for optional referential checks
    dim_series_ids: set[str] | None = None
    calendar_ds: set[str] | None = None

    dim_series_path = gold_dir / "dim_series.parquet"
    if dim_series_path.exists():
        try:
            t = pq.read_table(dim_series_path, columns=["id"])
            dim_series_ids = {x for x in t.column(0).to_pylist() if x is not None}
        except Exception:
            dim_series_ids = None

    dim_calendar_path = gold_dir / "dim_calendar.parquet"
    if dim_calendar_path.exists():
        try:
            t = pq.read_table(dim_calendar_path, columns=["d"])
            calendar_ds = {x for x in t.column(0).to_pylist() if x is not None}
        except Exception:
            calendar_ds = None

    for fname, required in GOLD_FILES:
        path = gold_dir / fname
        checks: list[DQCheck] = []

        if not path.exists():
            status = "missing"
            fr = DQFileReport(
                name=fname,
                status=status,
                path=str(path),
                rows=None,
                columns=None,
                bytes=None,
                sha256=None,
                checks=[
                    DQCheck(
                        name="exists",
                        status="fail" if required else "warn",
                        details={"required": required},
                    )
                ],
            )
            file_reports.append(fr)

            if required:
                overall = "fail"
                failures.append(f"missing required file: {fname}")
            else:
                if overall == "ok":
                    overall = "warn"
            continue

        meta = pq.read_metadata(path)
        rows = int(meta.num_rows)
        cols = int(meta.num_columns)
        size = int(path.stat().st_size)
        digest = sha256_file(path)

        # Basic non-empty
        checks.append(
            DQCheck(
                name="non_empty",
                status="pass" if rows > 0 else "fail",
                details={"rows": rows},
            )
        )
        if rows == 0:
            overall = "fail"
            failures.append(f"empty file: {fname}")

        # File-specific checks
        if fname == "dim_calendar.parquet":
            # Expect unique "d" and non-null
            try:
                nulls = _count_nulls(path, "d")
                distinct = _distinct_count_small(path, "d")
                dupes = rows - distinct

                checks.append(
                    DQCheck(
                        name="key_not_null:d",
                        status="pass" if nulls == 0 else "fail",
                        details={"nulls": nulls},
                    )
                )
                checks.append(
                    DQCheck(
                        name="key_unique:d",
                        status="pass" if dupes == 0 else "fail",
                        details={"rows": rows, "distinct": distinct, "dupes": dupes},
                    )
                )
                if nulls != 0 or dupes != 0:
                    overall = "fail"
                    failures.append("dim_calendar key issue (d)")
            except Exception as err:
                checks.append(
                    DQCheck(
                        name="calendar_checks",
                        status="warn",
                        details={"error": repr(err)},
                    )
                )
                if overall == "ok":
                    overall = "warn"

        if fname == "dim_series.parquet":
            # Expect unique "id" and non-null
            try:
                nulls = _count_nulls(path, "id")
                distinct = _distinct_count_small(path, "id")
                dupes = rows - distinct

                checks.append(
                    DQCheck(
                        name="key_not_null:id",
                        status="pass" if nulls == 0 else "fail",
                        details={"nulls": nulls},
                    )
                )
                checks.append(
                    DQCheck(
                        name="key_unique:id",
                        status="pass" if dupes == 0 else "fail",
                        details={"rows": rows, "distinct": distinct, "dupes": dupes},
                    )
                )
                if nulls != 0 or dupes != 0:
                    overall = "fail"
                    failures.append("dim_series key issue (id)")
            except Exception as err:
                checks.append(
                    DQCheck(
                        name="series_checks",
                        status="warn",
                        details={"error": repr(err)},
                    )
                )
                if overall == "ok":
                    overall = "warn"

        if fname == "fact_sell_prices.parquet":
            # Numeric sanity on sell_price if present
            try:
                min_v, max_v, nulls = _min_max_nulls_numeric(path, "sell_price")
                ok_nonneg = (min_v is None) or (min_v >= 0)

                checks.append(
                    DQCheck(
                        name="sell_price_not_null",
                        status="pass" if nulls == 0 else "warn",
                        details={"nulls": nulls},
                    )
                )
                checks.append(
                    DQCheck(
                        name="sell_price_non_negative",
                        status="pass" if ok_nonneg else "fail",
                        details={"min": min_v, "max": max_v},
                    )
                )
                if not ok_nonneg:
                    overall = "fail"
                    failures.append("negative sell_price found")
            except Exception as err:
                checks.append(
                    DQCheck(
                        name="sell_price_checks",
                        status="warn",
                        details={"error": repr(err)},
                    )
                )
                if overall == "ok":
                    overall = "warn"

        if fname == "fact_sales_long_sample.parquet":
            # Optional referential checks if we can load keys
            try:
                t = pq.read_table(path)
                cols_present = set(t.column_names)

                if "id" in cols_present and dim_series_ids is not None:
                    ids = {x for x in t["id"].to_pylist() if x is not None}
                    missing = len(ids - dim_series_ids)
                    checks.append(
                        DQCheck(
                            name="fk_series:id_in_dim_series",
                            status="pass" if missing == 0 else "warn",
                            details={"missing": missing},
                        )
                    )

                if "d" in cols_present and calendar_ds is not None:
                    ds = {x for x in t["d"].to_pylist() if x is not None}
                    missing = len(ds - calendar_ds)
                    checks.append(
                        DQCheck(
                            name="fk_calendar:d_in_dim_calendar",
                            status="pass" if missing == 0 else "warn",
                            details={"missing": missing},
                        )
                    )
            except Exception as err:
                checks.append(
                    DQCheck(
                        name="sales_sample_checks",
                        status="warn",
                        details={"error": repr(err)},
                    )
                )
                if overall == "ok":
                    overall = "warn"

        fr = DQFileReport(
            name=fname,
            status="ok",
            path=str(path),
            rows=rows,
            columns=cols,
            bytes=size,
            sha256=digest,
            checks=checks,
        )
        file_reports.append(fr)

    finished = datetime.now(timezone.utc)

    if failures:
        notes["failures"] = failures

    report = DQReport(
        pipeline="dq_m5",
        dataset_id=DATASET_ID,
        status=overall,
        started_at_utc=started.isoformat(),
        finished_at_utc=finished.isoformat(),
        config_path=str(cfg["config_path"]),
        gold_dir=str(gold_dir),
        report_path=str(report_path),
        files=file_reports,
        notes=notes,
    )

    report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")

    if overall == "fail" and strict:
        raise RuntimeError("DQ failed. See outputs/reports/dq_m5.json for details.")

    return report_path
