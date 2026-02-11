from __future__ import annotations

from pathlib import Path

import typer
from rich import print as rprint

from retail_ops_mlops.utils.config import ensure_dirs, load_config
from retail_ops_mlops.utils.logging import setup_logging

app = typer.Typer(no_args_is_help=True)

# Create Typer OptionInfo objects once (avoids Ruff B008)
CONFIG_OPTION = typer.Option(
    "configs/default.yaml",
    "--config",
    "-c",
    help="Path to YAML config file.",
)
STRICT_OPTION = typer.Option(
    True,
    "--strict/--no-strict",
    help="Fail if required inputs are missing.",
)
FORCE_OPTION = typer.Option(
    False,
    "--force",
    help="Overwrite existing outputs if they exist.",
)
ZIP_PATH_OPTION = typer.Option(
    None,
    "--zip-path",
    help="Optional path to the Kaggle zip file (if not already in data/raw/m5).",
)


@app.command("show-paths")
def show_paths(config: Path = CONFIG_OPTION) -> None:
    """Print resolved absolute paths from the config."""
    cfg = load_config(config)
    setup_logging(cfg.get("logging", {}).get("level", "INFO"))

    rprint(f"[bold]Config:[/bold] {cfg['config_path']}")
    rprint(f"[bold]Project root:[/bold] {cfg['project_root']}\n")

    rprint("[bold]Paths:[/bold]")
    for k, p in cfg["paths"].items():
        rprint(f"  - {k}: {p}")


@app.command("ensure-dirs")
def ensure_dirs_cmd(config: Path = CONFIG_OPTION) -> None:
    """Create required data/outputs directories (idempotent)."""
    cfg = load_config(config)
    setup_logging(cfg.get("logging", {}).get("level", "INFO"))
    ensure_dirs(cfg)
    rprint("[green]OK:[/green] ensured directories.")


@app.command("ingest-m5")
def ingest_m5(
    config: Path = CONFIG_OPTION,
    zip_path: Path | None = ZIP_PATH_OPTION,
    strict: bool = STRICT_OPTION,
) -> None:
    """Ingest M5 zip into data/raw/m5 and write an ingest report."""
    cfg = load_config(config)
    setup_logging(cfg.get("logging", {}).get("level", "INFO"))

    from retail_ops_mlops.pipelines.ingest_m5 import run as ingest_run

    try:
        report_path = ingest_run(config_path=config, zip_path=zip_path, strict=strict)
    except FileNotFoundError as err:
        rprint(f"[red]ERROR:[/red] {err}")
        raise typer.Exit(code=1) from err

    rprint(f"[green]OK:[/green] wrote report: {report_path}")


@app.command("bronze-m5")
def bronze_m5(
    config: Path = CONFIG_OPTION,
    force: bool = FORCE_OPTION,
    strict: bool = STRICT_OPTION,
) -> None:
    """Convert raw extracted M5 CSVs into bronze Parquet files + write a report."""
    cfg = load_config(config)
    setup_logging(cfg.get("logging", {}).get("level", "INFO"))

    from retail_ops_mlops.pipelines.bronze_m5 import run as bronze_run

    try:
        report_path = bronze_run(config_path=config, force=force, strict=strict)
    except FileNotFoundError as err:
        rprint(f"[red]ERROR:[/red] {err}")
        raise typer.Exit(code=1) from err

    rprint(f"[green]OK:[/green] wrote report: {report_path}")


if __name__ == "__main__":
    app()
