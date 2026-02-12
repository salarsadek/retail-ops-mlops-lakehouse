from __future__ import annotations

import os
import subprocess
from pathlib import Path


def _powershell_exe() -> str:
    for cand in ("pwsh", "powershell"):
        try:
            subprocess.run(
                [cand, "-NoProfile", "-Command", "$PSVersionTable.PSVersion"],
                check=True,
                capture_output=True,
                text=True,
            )
            return cand
        except Exception:
            continue
    return "powershell"


def _venv_python(root: Path) -> str:
    venv = os.environ.get("VIRTUAL_ENV")
    if venv:
        cand = Path(venv) / "Scripts" / "python.exe"
        if cand.exists():
            return str(cand)

    cand2 = root / ".venv" / "Scripts" / "python.exe"
    if cand2.exists():
        return str(cand2)

    return "python"


def _ensure_minimal_gold(root: Path) -> None:
    """
    CI kör i en ren checkout utan data. För att smoke testet ska vara stabilt
    skapar vi en minimal features-parquet om den saknas.
    """
    gold = root / "data" / "processed" / "m5" / "gold"
    gold.mkdir(parents=True, exist_ok=True)

    features_path = gold / "fact_sales_features_sample.parquet"
    if features_path.exists():
        return

    py = _venv_python(root)

    code = r"""
from pathlib import Path
import pandas as pd

path = Path(r"__PATH__")
path.parent.mkdir(parents=True, exist_ok=True)

# Minimal men realistisk schema för baseline-träning.
# (Om din pipeline kräver fler kolumner kan vi lägga till dem efter nästa CI-logg.)
df = pd.DataFrame(
    {
        "ds": pd.to_datetime(["2016-01-01", "2016-01-02", "2016-01-03"]),
        "y": [10.0, 12.0, 11.0],
        "snap_CA": [0, 0, 0],
        "sell_price": [1.0, 1.0, 1.0],
        "lag_28": [10.0, 10.0, 10.0],
        "rolling_mean_7": [10.0, 10.0, 10.0],
    }
)

df.to_parquet(path, index=False)
print("WROTE", path)
"""
    code = code.replace("__PATH__", str(features_path).replace("\\", "\\\\"))

    subprocess.run([py, "-c", code], check=True, cwd=str(root))


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
