from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from retail_ops_mlops.utils.config import ensure_dirs, load_config

logger = logging.getLogger(__name__)

DATASET_ID = "m5"
DEFAULT_COMPETITION = "m5-forecasting-accuracy"


@dataclass
class DownloadReport:
    pipeline: str
    dataset_id: str
    competition: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    config_path: str
    raw_dir: str
    zip_path: str | None
    zip_bytes: int | None
    zip_sha256: str | None
    report_path: str
    notes: dict[str, Any]


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _resolve_zip(raw_dir: Path, competition: str) -> Path | None:
    expected = raw_dir / f"{competition}.zip"
    if expected.exists():
        return expected

    # Fallback: if kaggle produced a zip with a different name, pick newest zip in raw_dir
    zips = sorted(raw_dir.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
    return zips[0] if zips else None


def _get_kaggle_api() -> Any:
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except Exception as err:
        raise RuntimeError(
            'Kaggle package not installed. Install with: python -m pip install -e ".[kaggle]"'
        ) from err

    api = KaggleApi()
    api.authenticate()
    return api


def run(
    config_path: str | Path = "configs/default.yaml",
    *,
    competition: str = DEFAULT_COMPETITION,
    force: bool = False,
    strict: bool = True,
) -> Path:
    """
    Download M5 Kaggle competition zip into data/raw/m5.

    Why separate from ingest?
    - download_m5 = network/auth step (external dependency)
    - ingest_m5    = unzip + record metadata (pure local + reproducible)
    """
    cfg = load_config(config_path)
    ensure_dirs(cfg)

    started = datetime.now(timezone.utc)

    raw_dir = cfg["paths"]["data_raw"] / DATASET_ID
    raw_dir.mkdir(parents=True, exist_ok=True)

    report_path = cfg["paths"]["outputs_reports"] / "download_m5.json"

    notes: dict[str, Any] = {
        "expected_token_location": str(Path.home() / ".kaggle" / "kaggle.json"),
        "tip_if_403": (
            "403 usually means you haven't accepted the competition rules. "
            "Open the competition page in your browser, click 'Join Competition', "
            "accept rules, then retry."
        ),
        "competition": competition,
        "force": force,
    }

    status = "ok"

    # If already downloaded and not forcing, skip network
    existing_zip = _resolve_zip(raw_dir, competition)
    if existing_zip is not None and existing_zip.exists() and not force:
        status = "exists"
        zip_path = existing_zip
    else:
        zip_path = None
        try:
            api = _get_kaggle_api()
            logger.info("Downloading Kaggle competition %s to %s", competition, raw_dir)
            api.competition_download_files(
                competition,
                path=str(raw_dir),
                force=force,
                quiet=False,
            )
            zip_path = _resolve_zip(raw_dir, competition)
        except Exception as err:
            status = "error"
            notes["error"] = repr(err)
            zip_path = _resolve_zip(raw_dir, competition)

    if zip_path is None or not zip_path.exists():
        if status == "ok":
            status = "missing_output"
        notes["missing_reason"] = "No zip found in data/raw/m5 after download."

    zip_bytes = zip_path.stat().st_size if zip_path and zip_path.exists() else None
    zip_sha256 = sha256_file(zip_path) if zip_path and zip_path.exists() else None

    finished = datetime.now(timezone.utc)

    report = DownloadReport(
        pipeline="download_m5",
        dataset_id=DATASET_ID,
        competition=competition,
        status=status,
        started_at_utc=started.isoformat(),
        finished_at_utc=finished.isoformat(),
        config_path=str(cfg["config_path"]),
        raw_dir=str(raw_dir),
        zip_path=str(zip_path) if zip_path else None,
        zip_bytes=zip_bytes,
        zip_sha256=zip_sha256,
        report_path=str(report_path),
        notes=notes,
    )

    report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")

    if status not in ("ok", "exists") and strict:
        raise RuntimeError(notes.get("tip_if_403", "Download failed."))

    return report_path
