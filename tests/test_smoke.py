from __future__ import annotations

import os
import subprocess
from pathlib import Path


def _powershell_exe() -> str:
    # Prefer pwsh if available (GitHub Actions ubuntu), else Windows PowerShell.
    return os.environ.get("POWERSHELL_EXE", "pwsh")


def _venv_python(root: Path) -> str:
    py = root / ".venv" / "Scripts" / "python.exe"
    if py.exists():
        return str(py)
    # Linux/macOS venv layout (CI ubuntu)
    py2 = root / ".venv" / "bin" / "python"
    if py2.exists():
        return str(py2)
    # Fallback: rely on PATH (CI uses setup-python)
    return "python"


def _ensure_minimal_gold(root: Path) -> None:
    """Create a small-but-robust gold features parquet for smoke tests.

    Goal: ensure eval_m5 has >=2 test rows AFTER dropping NaNs.
    So we create a longer series with fully-populated lag/rolling columns,
    and mark the last 10 rows as test.
    """
    features_path = (
        root / "data" / "processed" / "m5" / "gold" / "fact_sales_features_sample.parquet"
    )

    py = _venv_python(root)

    # NOTE: use f-string only for the path; keep code as raw python.
    code = f"""
from pathlib import Path
import pandas as pd

path = Path(r\"{str(features_path)}\")
path.parent.mkdir(parents=True, exist_ok=True)

n = 60
dates = pd.date_range("2016-01-01", periods=n, freq="D")

df = pd.DataFrame({{
    "id": ["FOO_1_CA_1"] * n,
    "d": [f"d_{{i+1}}" for i in range(n)],
    "sales": [float(10 + (i % 5)) for i in range(n)],
    "d_num": list(range(1, n+1)),
    # last 10 rows are test
    "is_test": [False] * (n - 10) + [True] * 10,

    # categorical ids expected by pipeline
    "item_id": ["FOO_1"] * n,
    "dept_id": ["FOO"] * n,
    "cat_id": ["FOO"] * n,
    "store_id": ["CA_1"] * n,
    "state_id": ["CA"] * n,

    # lag/rolling features expected by pipeline (pre-filled, no NaNs)
    "lag_1": [10.0] * n,
    "lag_7": [10.0] * n,
    "lag_28": [10.0] * n,
    "roll_mean_7": [10.0] * n,
    "roll_mean_28": [10.0] * n,

    # calendar-ish
    "wm_yr_wk": [11111] * n,
    "wday": [1] * n,
    "month": [1] * n,
    "year": [2016] * n,

    # price + SNAP
    "sell_price": [1.0] * n,
    "snap_CA": [0] * n,
    "snap_TX": [0] * n,
    "snap_WI": [0] * n,

    # events
    "event_name_1": [None] * n,
    "event_type_1": [None] * n,
    "event_name_2": [None] * n,
    "event_type_2": [None] * n,
}})

df.to_parquet(path, index=False)
print("WROTE", path)
"""

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
