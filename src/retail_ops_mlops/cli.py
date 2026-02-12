from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich import print as rprint

app = typer.Typer(no_args_is_help=True)

DEFAULT_CONFIG_PATH = Path("configs/default.yaml")


@app.command("show-paths")
def show_paths(
    config: Annotated[Path, typer.Option("--config")] = DEFAULT_CONFIG_PATH,
) -> None:
    """Print resolved absolute paths from the config."""
    from retail_ops_mlops.utils.config import load_cfg

    cfg = load_cfg(config)
    paths = cfg.get("paths", {})
    rprint("[bold]paths[/bold]:")
    for k, v in paths.items():
        rprint(f"  - {k}: {Path(v).resolve()}")


@app.command("ensure-dirs")
def ensure_dirs(
    config: Annotated[Path, typer.Option("--config")] = DEFAULT_CONFIG_PATH,
) -> None:
    """Create required data/outputs directories (idempotent)."""
    from retail_ops_mlops.utils.config import ensure_project_dirs, load_cfg

    cfg = load_cfg(config)
    ensure_project_dirs(cfg, root=Path.cwd())
    rprint("[green]OK[/green]: ensured directories")


@app.command("build-features-m5")
def build_features_m5(
    config: Annotated[Path, typer.Option("--config")] = DEFAULT_CONFIG_PATH,
    horizon: Annotated[int, typer.Option("--horizon")] = 28,
    force: Annotated[bool, typer.Option("--force")] = False,
) -> None:
    """Build M5 features table (gold) used by train/eval."""
    from retail_ops_mlops.pipelines.build_features_m5 import run

    report = run(config_path=config, horizon=horizon, force=force)
    rprint(f"[green]OK[/green]: wrote report: {report}")


@app.command("dq-m5")
def dq_m5(
    config: Annotated[Path, typer.Option("--config")] = DEFAULT_CONFIG_PATH,
    horizon: Annotated[int, typer.Option("--horizon")] = 28,
    force: Annotated[bool, typer.Option("--force")] = False,
) -> None:
    """Run DQ checks for M5 features table (fails fast on bad data)."""
    from retail_ops_mlops.pipelines.dq_m5 import run

    report = run(config_path=config, horizon=horizon, force=force)
    rprint(f"[green]OK[/green]: wrote report: {report}")


@app.command("train-m5")
def train_m5(
    config: Annotated[Path, typer.Option("--config")] = DEFAULT_CONFIG_PATH,
    horizon: Annotated[int, typer.Option("--horizon")] = 28,
    force: Annotated[bool, typer.Option("--force")] = False,
) -> None:
    """Train baseline model on M5 gold sample and write artifacts."""
    from retail_ops_mlops.pipelines.train_m5 import run

    report = run(config_path=config, horizon=horizon, force=force)
    rprint(f"[green]OK[/green]: wrote report: {report}")


@app.command("eval-m5")
def eval_m5(
    config: Annotated[Path, typer.Option("--config")] = DEFAULT_CONFIG_PATH,
    horizon: Annotated[int, typer.Option("--horizon")] = 28,
    force: Annotated[bool, typer.Option("--force")] = False,
) -> None:
    """Evaluate trained M5 baseline and write figures/tables/reports."""
    from retail_ops_mlops.pipelines.eval_m5 import run

    report = run(config_path=config, horizon=horizon, force=force)
    rprint(f"[green]OK[/green]: wrote report: {report}")


if __name__ == "__main__":
    app()
