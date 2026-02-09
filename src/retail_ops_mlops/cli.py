from __future__ import annotations

from pathlib import Path

import typer
from rich import print as rprint

from retail_ops_mlops.utils.config import ensure_dirs, load_config
from retail_ops_mlops.utils.logging import setup_logging

app = typer.Typer(no_args_is_help=True)

# B008-safe: create OptionInfo once, reuse as default in commands
CONFIG_OPTION = typer.Option(
    "configs/default.yaml",
    "--config",
    "-c",
    help="Path to YAML config file.",
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


if __name__ == "__main__":
    app()
