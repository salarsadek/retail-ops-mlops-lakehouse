from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


def _powershell_exe() -> str:
    exe = shutil.which("pwsh")
    if exe:
        return exe
    exe = shutil.which("powershell")
    if exe:
        return exe
    return "pwsh"


def _venv_python(root: Path) -> str:
    win = root / ".venv" / "Scripts" / "python.exe"
    if win.exists():
        return str(win)
    return "python"


def _ensure_minimal_gold(root: Path) -> None:
    """
    Create the minimal parquet file expected by train_m5 pipeline.

    Keep this as a *superset* of required columns; extra columns are fine.
    """
    gold_dir = root / "data" / "processed" / "m5" / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    features_path = gold_dir / "fact_sales_features_sample.parquet"

    py = _venv_python(root)

    code = r"""
import os
from pathlib import Path
import pandas as pd

path = Path(os.environ["FEATURES_PATH"])
path.parent.mkdir(parents=True, exist_ok=True)

df = pd.DataFrame(
    {
        # Required core identifiers/targets
        "id": ["FOO_1_CA_1", "FOO_1_CA_1", "FOO_1_CA_1"],
        "d": ["d_1", "d_2", "d_3"],
        "sales": [10.0, 12.0, 11.0],
        "d_num": [1, 2, 3],
        "is_test": [False, False, True],

        # Typical categorical dimensions (often used as categoricals)
        "item_id": ["FOO_1", "FOO_1", "FOO_1"],
        "dept_id": ["FOO", "FOO", "FOO"],
        "cat_id": ["FOODS", "FOODS", "FOODS"],
        "store_id": ["CA_1", "CA_1", "CA_1"],
        "state_id": ["CA", "CA", "CA"],

        # Typical numeric lag/rolling features expected by baseline pipelines
        "lag_1": [10.0, 10.0, 12.0],
        "lag_7": [10.0, 10.0, 10.0],
        "lag_28": [10.0, 10.0, 10.0],
        "roll_mean_7": [10.0, 10.5, 11.0],
        "roll_mean_28": [10.0, 10.0, 10.0],

        # Time/calendar features
        "wm_yr_wk": [11101, 11101, 11101],
        "wday": [1, 2, 3],
        "month": [1, 1, 1],
        "year": [2016, 2016, 2016],

        # Price / SNAP features (commonly referenced)
        "sell_price": [1.0, 1.0, 1.0],
        "snap_CA": [0, 0, 0],
        "snap_TX": [0, 0, 0],
        "snap_WI": [0, 0, 0],

        # Events (if pipeline expects them)
        "event_name_1": ["None", "None", "None"],
        "event_type_1": ["None", "None", "None"],
        "event_name_2": ["None", "None", "None"],
        "event_type_2": ["None", "None", "None"],
    }
)

df.to_parquet(path, index=False)
print("WROTE", path)
"""
    env = os.environ.copy()
    env["FEATURES_PATH"] = str(features_path)

    subprocess.run([py, "-c", code], check=True, cwd=str(root), env=env)


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
