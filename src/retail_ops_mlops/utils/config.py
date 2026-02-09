from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def get_project_root() -> Path:
    """
    Returns the repository root.
    This file lives in: repo/src/retail_ops_mlops/utils/config.py
    So repo root is parents[3].
    """
    return Path(__file__).resolve().parents[3]


def _expand_env(value: Any) -> Any:
    if isinstance(value, str):
        return os.path.expandvars(value)
    return value


def load_config(config_path: str | Path = "configs/default.yaml") -> dict[str, Any]:
    """
    Load YAML config and resolve paths to absolute Paths.
    """
    root = get_project_root()
    cfg_path = Path(config_path)
    if not cfg_path.is_absolute():
        cfg_path = (root / cfg_path).resolve()

    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    if not isinstance(cfg, dict):
        raise ValueError(f"Config must be a dict, got {type(cfg)}")

    # Expand env vars in top-level keys (shallow) + nested dicts
    for k, v in list(cfg.items()):
        if isinstance(v, dict):
            cfg[k] = {kk: _expand_env(vv) for kk, vv in v.items()}
        else:
            cfg[k] = _expand_env(v)

    # Resolve paths section into absolute Path objects
    paths = cfg.get("paths", {})
    if not isinstance(paths, dict):
        raise ValueError("Config key 'paths' must be a dict")

    resolved: dict[str, Path] = {}
    for key, val in paths.items():
        if not isinstance(val, str):
            raise ValueError(f"paths.{key} must be a string path, got {type(val)}")
        p = Path(os.path.expandvars(val))
        if not p.is_absolute():
            p = (root / p).resolve()
        resolved[key] = p

    cfg["paths"] = resolved
    cfg["project_root"] = root
    cfg["config_path"] = cfg_path
    return cfg


def ensure_dirs(cfg: dict[str, Any], keys: list[str] | None = None) -> None:
    """
    Ensure important directories exist. Safe to call repeatedly.
    """
    default_keys = [
        "data_raw",
        "data_interim",
        "data_processed",
        "outputs",
        "outputs_figures",
        "outputs_tables",
        "outputs_models",
        "outputs_reports",
    ]
    use_keys = keys or default_keys
    paths: dict[str, Path] = cfg["paths"]

    for k in use_keys:
        paths[k].mkdir(parents=True, exist_ok=True)
