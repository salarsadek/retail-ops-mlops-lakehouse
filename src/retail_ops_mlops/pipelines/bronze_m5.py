from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pc
import pyarrow.parquet as pq

from retail_ops_mlops.utils.config import ensure_dirs, load_config

logger = logging.getLogger(__name__)

DATASET_ID = "m5"
EXPECTED_FILES = (
    "calendar.csv",
    "sales_train_evaluation.csv",
    "sales_train_validation.csv",
    "sample_submission.csv",
    "sell_prices.csv",
)


@dataclass
class BronzeFileReport:
    name: str
    status: str
    csv_path: str
    parquet_path: str | None
    rows: int | None
    csv_bytes: int | None
    parquet_bytes: int | None
    csv_sha256: str | None
    parquet_sha256: str | None


@dataclass
class BronzeReport:
    pipeline: str
    dataset_id: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    config_path: str
    raw_extracted_dir: str
    bronze_dir: str
    report_path: str
    files: list[BronzeFileReport]
    notes: dict[str, Any]


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def csv_to_parquet_stream(
    csv_path: Path,
    parquet_path: Path,
    *,
    compression: str = "snappy",
) -> int:
    """Stream CSV -> Parquet in batches to avoid loading huge CSVs into RAM."""
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    read_options = pc.ReadOptions(block_size=1 << 20, use_threads=True)
    parse_options = pc.ParseOptions(delimiter=",", quote_char='"', double_quote=True)
    convert_options = pc.ConvertOptions()

    reader = pc.open_csv(
        csv_path,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )

    writer: pq.ParquetWriter | None = None
    rows = 0

    try:
        while True:
            batch = reader.read_next_batch()
            rows += batch.num_rows
            table = pa.Table.from_batches([batch])

            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema, compression=compression)

            writer.write_table(table)
    except StopIteration:
        pass
    finally:
        if writer is not None:
            writer.close()

    return rows


def run(
    config_path: str | Path = "configs/default.yaml",
    *,
    force: bool = False,
    strict: bool = True,
) -> Path:
    """
    Bronze layer for M5:
    - Input:  data/raw/m5/extracted/*.csv (immutable raw)
    - Output: data/interim/m5/bronze/*.parquet
    - Report: outputs/reports/bronze_m5.json
    """
    cfg = load_config(config_path)
    ensure_dirs(cfg)

    started = datetime.now(timezone.utc)

    raw_extracted = cfg["paths"]["data_raw"] / DATASET_ID / "extracted"
    bronze_dir = cfg["paths"]["data_interim"] / DATASET_ID / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)

    report_path = cfg["paths"]["outputs_reports"] / "bronze_m5.json"

    notes: dict[str, Any] = {
        "bronze_definition": "Raw-but-optimized (CSV->Parquet). No business cleaning here.",
        "input_dir": str(raw_extracted),
        "output_dir": str(bronze_dir),
        "force_overwrite": force,
    }

    file_reports: list[BronzeFileReport] = []
    overall_status = "ok"

    if not raw_extracted.exists():
        overall_status = "missing_input"
        notes["missing_reason"] = "Expected raw extracted dir not found. Run ingest-m5 first."

        for name in EXPECTED_FILES:
            csv_path = raw_extracted / name
            file_reports.append(
                BronzeFileReport(
                    name=name,
                    status="missing",
                    csv_path=str(csv_path),
                    parquet_path=None,
                    rows=None,
                    csv_bytes=None,
                    parquet_bytes=None,
                    csv_sha256=None,
                    parquet_sha256=None,
                )
            )
    else:
        for name in EXPECTED_FILES:
            csv_path = raw_extracted / name
            parquet_path = bronze_dir / name.replace(".csv", ".parquet")

            if not csv_path.exists():
                overall_status = "partial"
                file_reports.append(
                    BronzeFileReport(
                        name=name,
                        status="missing",
                        csv_path=str(csv_path),
                        parquet_path=None,
                        rows=None,
                        csv_bytes=None,
                        parquet_bytes=None,
                        csv_sha256=None,
                        parquet_sha256=None,
                    )
                )
                continue

            if parquet_path.exists() and not force:
                rows = pq.read_metadata(parquet_path).num_rows
                file_reports.append(
                    BronzeFileReport(
                        name=name,
                        status="exists",
                        csv_path=str(csv_path),
                        parquet_path=str(parquet_path),
                        rows=rows,
                        csv_bytes=csv_path.stat().st_size,
                        parquet_bytes=parquet_path.stat().st_size,
                        csv_sha256=sha256_file(csv_path),
                        parquet_sha256=sha256_file(parquet_path),
                    )
                )
                continue

            logger.info("Bronze: %s -> %s", csv_path.name, parquet_path.name)
            rows = csv_to_parquet_stream(csv_path, parquet_path)

            file_reports.append(
                BronzeFileReport(
                    name=name,
                    status="ok",
                    csv_path=str(csv_path),
                    parquet_path=str(parquet_path),
                    rows=rows,
                    csv_bytes=csv_path.stat().st_size,
                    parquet_bytes=parquet_path.stat().st_size,
                    csv_sha256=sha256_file(csv_path),
                    parquet_sha256=sha256_file(parquet_path),
                )
            )

    finished = datetime.now(timezone.utc)

    report = BronzeReport(
        pipeline="bronze_m5",
        dataset_id=DATASET_ID,
        status=overall_status,
        started_at_utc=started.isoformat(),
        finished_at_utc=finished.isoformat(),
        config_path=str(cfg["config_path"]),
        raw_extracted_dir=str(raw_extracted),
        bronze_dir=str(bronze_dir),
        report_path=str(report_path),
        files=file_reports,
        notes=notes,
    )

    report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")

    if overall_status != "ok" and strict:
        raise FileNotFoundError(notes.get("missing_reason", "Bronze pipeline failed."))

    return report_path
