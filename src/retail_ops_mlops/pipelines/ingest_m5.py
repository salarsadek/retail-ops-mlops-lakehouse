from __future__ import annotations

import hashlib
import json
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from retail_ops_mlops.utils.config import ensure_dirs, load_config

DATASET_ID = "m5"
EXPECTED_ZIP_NAMES = ("m5-forecasting-accuracy.zip", "m5.zip")


@dataclass
class IngestReport:
    pipeline: str
    dataset_id: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    config_path: str
    raw_dir: str
    zip_path: str | None
    extracted_dir: str | None
    extracted_files: list[dict[str, Any]]
    notes: dict[str, Any]


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def run(
    config_path: str | Path = "configs/default.yaml",
    zip_path: str | Path | None = None,
    strict: bool = True,
) -> Path:
    """
    Ingest M5 dataset into data/raw/m5.

    Behavior:
    - If zip_path is None, we search in data/raw/m5 for common zip names.
    - We always write an ingest report to outputs/reports/ingest_m5.json.
    - If strict=True and the zip is missing, we raise after writing the report.
    """
    cfg = load_config(config_path)
    ensure_dirs(cfg)

    started = datetime.now(timezone.utc)

    raw_dir = cfg["paths"]["data_raw"] / DATASET_ID
    raw_dir.mkdir(parents=True, exist_ok=True)

    report_path = cfg["paths"]["outputs_reports"] / "ingest_m5.json"

    # If zip_path not provided, search in raw_dir for expected names
    resolved_zip: Path | None = None
    if zip_path is not None:
        p = Path(zip_path)
        resolved_zip = p if p.is_absolute() else (cfg["project_root"] / p).resolve()
    else:
        for name in EXPECTED_ZIP_NAMES:
            cand = raw_dir / name
            if cand.exists():
                resolved_zip = cand
                break

    extracted_dir: Path | None = None
    extracted_files: list[dict[str, Any]] = []
    status = "ok"

    notes: dict[str, Any] = {
        "expected_zip_locations": [str((raw_dir / n).resolve()) for n in EXPECTED_ZIP_NAMES],
        "how_to_provide_data": (
            "Place the Kaggle zip file in data/raw/m5/ (e.g. m5-forecasting-accuracy.zip) "
            "OR pass --zip-path to the CLI (we'll add that next)."
        ),
        "raw_is_immutable": "We do not clean/transform here; only store + document.",
    }

    if resolved_zip is None or not resolved_zip.exists():
        status = "missing_input"
        notes["missing_reason"] = "Dataset zip not found."
    else:
        extracted_dir = raw_dir / "extracted"
        extracted_dir.mkdir(parents=True, exist_ok=True)

        # Extract
        with zipfile.ZipFile(resolved_zip, "r") as zf:
            zf.extractall(extracted_dir)

        # Collect file metadata (size + sha256)
        for fp in sorted(extracted_dir.rglob("*")):
            if fp.is_file():
                extracted_files.append(
                    {
                        "path": str(fp.relative_to(extracted_dir)).replace("\\", "/"),
                        "bytes": fp.stat().st_size,
                        "sha256": sha256_file(fp),
                    }
                )

        notes["zip_bytes"] = resolved_zip.stat().st_size
        notes["zip_sha256"] = sha256_file(resolved_zip)

    finished = datetime.now(timezone.utc)

    report = IngestReport(
        pipeline="ingest_m5",
        dataset_id=DATASET_ID,
        status=status,
        started_at_utc=started.isoformat(),
        finished_at_utc=finished.isoformat(),
        config_path=str(cfg["config_path"]),
        raw_dir=str(raw_dir),
        zip_path=str(resolved_zip) if resolved_zip else None,
        extracted_dir=str(extracted_dir) if extracted_dir else None,
        extracted_files=extracted_files,
        notes=notes,
    )

    report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")

    if status != "ok" and strict:
        raise FileNotFoundError(notes["how_to_provide_data"])

    return report_path
