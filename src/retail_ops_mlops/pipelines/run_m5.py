from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from retail_ops_mlops.utils.config import ensure_dirs, load_config

logger = logging.getLogger(__name__)

DATASET_ID = "m5"
SUCCESS_STATUSES = {"ok", "exists"}


@dataclass
class StageSummary:
    pipeline: str
    status: str
    report_path: str | None
    error: str | None


@dataclass
class RunReport:
    pipeline: str
    dataset_id: str
    status: str
    started_at_utc: str
    finished_at_utc: str
    config_path: str
    report_path: str
    stages: list[StageSummary]
    notes: dict[str, Any]


def _read_status(report_path: Path) -> str:
    try:
        data = json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return "unknown"
    status = data.get("status")
    return str(status) if status is not None else "unknown"


def run(
    config_path: str | Path = "configs/default.yaml",
    *,
    zip_path: Path | None = None,
    force: bool = False,
    strict: bool = True,
) -> Path:
    """
    Run full M5 pipeline in order:
      download -> ingest -> bronze -> silver -> gold

    Design:
    - Stage pipelines run with strict=False so they always write their own report.
    - This orchestrator stops at first failure, writes run_m5.json, and raises if strict=True.
    """
    cfg = load_config(config_path)
    ensure_dirs(cfg)

    started = datetime.now(timezone.utc)
    report_path = cfg["paths"]["outputs_reports"] / "run_m5.json"

    notes: dict[str, Any] = {
        "stop_on_first_failure": True,
        "force_overwrite": force,
        "zip_path": str(zip_path) if zip_path else None,
        "success_statuses": sorted(SUCCESS_STATUSES),
        "stage_order": [
            "download_m5",
            "ingest_m5",
            "bronze_m5",
            "silver_m5",
            "gold_m5",
        ],
    }

    from retail_ops_mlops.pipelines.bronze_m5 import run as bronze_run
    from retail_ops_mlops.pipelines.download_m5 import run as download_run
    from retail_ops_mlops.pipelines.gold_m5 import run as gold_run
    from retail_ops_mlops.pipelines.ingest_m5 import run as ingest_run
    from retail_ops_mlops.pipelines.silver_m5 import run as silver_run

    stage_defs: list[tuple[str, Any, dict[str, Any]]] = [
        ("download_m5", download_run, {"force": force}),
        ("ingest_m5", ingest_run, {"zip_path": zip_path}),
        ("bronze_m5", bronze_run, {"force": force}),
        ("silver_m5", silver_run, {"force": force}),
        ("gold_m5", gold_run, {"force": force}),
    ]

    stages: list[StageSummary] = []
    failed = False

    for pipeline, fn, kwargs in stage_defs:
        if failed:
            stages.append(
                StageSummary(
                    pipeline=pipeline,
                    status="skipped",
                    report_path=None,
                    error="Skipped due to earlier failure.",
                )
            )
            continue

        logger.info("Run: %s", pipeline)
        try:
            rp: Path = fn(config_path=config_path, strict=False, **kwargs)
            status = _read_status(rp)

            stages.append(
                StageSummary(
                    pipeline=pipeline,
                    status=status,
                    report_path=str(rp),
                    error=None,
                )
            )

            if status not in SUCCESS_STATUSES:
                failed = True
        except Exception as err:
            stages.append(
                StageSummary(
                    pipeline=pipeline,
                    status="error",
                    report_path=None,
                    error=repr(err),
                )
            )
            failed = True

    finished = datetime.now(timezone.utc)

    overall_status = "ok"
    if any(s.status not in SUCCESS_STATUSES for s in stages):
        overall_status = "error"

    report = RunReport(
        pipeline="run_m5",
        dataset_id=DATASET_ID,
        status=overall_status,
        started_at_utc=started.isoformat(),
        finished_at_utc=finished.isoformat(),
        config_path=str(cfg["config_path"]),
        report_path=str(report_path),
        stages=stages,
        notes=notes,
    )

    report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")

    if strict and overall_status != "ok":
        raise RuntimeError(f"run_m5 failed; see report: {report_path}")

    return report_path
