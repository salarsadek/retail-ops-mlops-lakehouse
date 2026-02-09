from __future__ import annotations

from pathlib import Path

import typer
from rich import print as rprint

from retail_ops_mlops.pipelines.ingest_m5 import run as ingest_m5_run
from retail_ops_mlops.utils.config import ensure_dirs, load_config
from retail_ops_mlops.utils.logging import setup_logging

app = typer.Typer(no_args_is_help=True)

# B008-safe: create OptionInfo once, reuse as defaults in commands
CONFIG_OPTION = typer.Option(
    "configs/default.yaml",
    "--config",
    "-c",
    help="Path to YAML config file.",
)

ZIP_PATH_OPTION = typer.Option(
    None,
    "--zip-path",
    help=(
        "Path to the M5 zip file. If omitted, the pipeline searches "
        "data/raw/m5/ for common zip names."
    ),
)

STRICT_OPTION = typer.Option(
    True,
    "--strict/--no-strict",
    help="If strict, fail when the zip is missing (useful for CI/orchestration).",
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
    """
    Ingest M5 dataset into the raw zone (data/raw/m5) and write an ingest report.
    """
    cfg = load_config(config)
    setup_logging(cfg.get("logging", {}).get("level", "INFO"))

    report_path = cfg["paths"]["outputs_reports"] / "ingest_m5.json"

    try:
        out = ingest_m5_run(config_path=config, zip_path=zip_path, strict=strict)
        rprint(f"[green]OK:[/green] wrote report: {out}")
    except FileNotFoundError as err:
        rprint(f"[red]ERROR:[/red] {err}")
        rprint(f"[yellow]Report written to:[/yellow] {report_path}")
        raise typer.Exit(code=1) from err


if __name__ == "__main__":
    app()
