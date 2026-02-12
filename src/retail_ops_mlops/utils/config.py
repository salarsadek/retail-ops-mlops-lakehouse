from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_cfg(config_path: Path) -> dict[str, Any]:
    """Load YAML config. Canonical function used by CLI."""
    p = Path(config_path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    data = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Config root must be a mapping/dict.")
    return data


# Backwards-compat alias (some modules may import load_config)
def load_config(config_path: Path) -> dict[str, Any]:
    return load_cfg(config_path)


def ensure_project_dirs(cfg: dict[str, Any], root: Path | None = None) -> None:
    """Create required project directories (idempotent)."""
    if "paths" not in cfg or not isinstance(cfg["paths"], dict):
        raise ValueError("Config must contain 'paths' mapping.")

    paths = cfg["paths"]

    # Root is used only for printing/consistency.
    # Paths should already be relative/absolute as in cfg._ = root or Path.cwd()

    required_keys = [
        "data_processed",
        "outputs_models",
        "outputs_reports",
        "outputs_tables",
        "outputs_figures",
    ]
    missing = [k for k in required_keys if k not in paths]
    if missing:
        raise ValueError(f"Missing required paths in config: {missing}")

    for k in required_keys:
        Path(paths[k]).mkdir(parents=True, exist_ok=True)
