from __future__ import annotations

import os
import subprocess
from pathlib import Path


def _powershell_exe() -> str:
    # In CI this is usually "powershell"; locally it might be "pwsh".
    # Try pwsh first if available.
    for cand in ("pwsh", "powershell"):
        try:
            subprocess.run(
                [cand, "-NoProfile", "-Command", "$PSVersionTable.PSVersion"],
                check=True,
                capture_output=True,
            )
            return cand
        except Exception:
            continue
    return "powershell"


def _venv_python(root: Path) -> str:
    # Prefer active venv if set
    venv = os.environ.get("VIRTUAL_ENV")
    if venv:
        cand = Path(venv) / "Scripts" / "python.exe"
        if cand.exists():
            return str(cand)

    # Prefer local .venv
    cand2 = root / ".venv" / "Scripts" / "python.exe"
    if cand2.exists():
        return str(cand2)

    # Fallback
    return "python"


def _ensure_minimal_gold(root: Path) -> None:
    # Minimal directory so the pipeline has a place to read/write.
    # If your repo creates gold files elsewhere, keep that logic there.
    gold = root / "data" / "processed" / "m5" / "gold"
    gold.mkdir(parents=True, exist_ok=True)


def test_smoke_run_m5_ps1() -> None:
    root = Path(__file__).resolve().parents[1]
    ps1 = root / "scripts" / "run_m5.ps1"
    assert ps1.exists(), f"Missing: {ps1}"

    _ensure_minimal_gold(root)

    shell = _powershell_exe()
    pyexe = _venv_python(root)

    cmd = [
        shell,
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(ps1),
        "-Force",
        "-PythonExe",
        pyexe,
    ]
    proc = subprocess.run(cmd, cwd=str(root), capture_output=True, text=True)
    if proc.returncode != 0:
        print("STDOUT:\n", proc.stdout)
        print("STDERR:\n", proc.stderr)
    assert proc.returncode == 0
