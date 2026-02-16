param(
  [switch]$Force,
  [string]$PythonExe = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-PythonExe {
  param([string]$Explicit)

  # If user provided something, accept either:
  #  - an existing file path
  #  - a command name resolvable via Get-Command (python/python3/etc.)
  if ($Explicit) {
    if (Test-Path $Explicit) {
      return [System.IO.Path]::GetFullPath($Explicit)
    }
    try {
      return (Get-Command $Explicit -ErrorAction Stop).Source
    } catch {
      $p = [System.IO.Path]::GetFullPath($Explicit)
      throw "PythonExe not found as path or command: '$Explicit' (resolved path tried: $p)"
    }
  }

  # Prefer active venv if set (Windows + Linux)
  if ($env:VIRTUAL_ENV) {
    $cands = @(
      (Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"),
      (Join-Path $env:VIRTUAL_ENV "Scripts\python"),
      (Join-Path $env:VIRTUAL_ENV "bin/python"),
      (Join-Path $env:VIRTUAL_ENV "bin/python3")
    )
    foreach ($c in $cands) { if (Test-Path $c) { return $c } }
  }

  # Prefer repo .venv (Windows + Linux)
  $repo = (Get-Location).Path
  $cands2 = @(
    (Join-Path $repo ".venv\Scripts\python.exe"),
    (Join-Path $repo ".venv\Scripts\python"),
    (Join-Path $repo ".venv\bin\python"),
    (Join-Path $repo ".venv\bin\python3")
  )
  foreach ($c in $cands2) { if (Test-Path $c) { return $c } }

  # Fallback: python3 then python
  foreach ($name in @("python3", "python")) {
    try { return (Get-Command $name -ErrorAction Stop).Source } catch { }
  }

  throw "Could not resolve a Python executable (tried venv + .venv + python3/python)."
}

function Invoke-Py {
  param(
    [Parameter(Mandatory=$true)][string]$Py,
    [Parameter(Mandatory=$true)][string[]]$Args
  )

  $cmd = "`"$Py`" " + ($Args -join " ")
  Write-Host $cmd -ForegroundColor DarkGray

  & $Py @Args
  if ($LASTEXITCODE -ne 0) {
    throw "Python command failed (exit=$LASTEXITCODE): $cmd"
  }
}

$py = Resolve-PythonExe -Explicit $PythonExe

Write-Host "== Retail Ops M5: RUN ==" -ForegroundColor Cyan
Write-Host ("python = " + $py) -ForegroundColor Gray

Write-Host "`n== ensure-dirs ==" -ForegroundColor Cyan
Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","ensure-dirs")

Write-Host "`n== train-m5 ==" -ForegroundColor Cyan
if ($Force) {
  Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","train-m5","--force")
} else {
  Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","train-m5")
}

Write-Host "`n== eval-m5 ==" -ForegroundColor Cyan
if ($Force) {
  Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","eval-m5","--force")
} else {
  Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","eval-m5")
}

Write-Host "`n== sanity: required artifacts exist ==" -ForegroundColor Cyan
$need = @(
  ".\outputs\models\m5_ridge_baseline.joblib",
  ".\outputs\reports\train_m5.json",
  ".\outputs\reports\eval_m5.json",
  ".\outputs\tables\eval_m5_metrics.csv",
  ".\outputs\tables\eval_m5_predictions.csv",
  ".\outputs\figures\eval_m5_pred_vs_true.pdf",
  ".\outputs\figures\eval_m5_residuals.pdf"
)

foreach ($p in $need) {
  if (-not (Test-Path $p)) { throw "Missing artifact: $p" }
}

Write-Host "`n== sanity: metrics preview ==" -ForegroundColor Cyan
Get-Content ".\outputs\tables\eval_m5_metrics.csv" -Raw

Write-Host "`nOK: run completed" -ForegroundColor Green

Write-Host ""
Write-Host "== dbt: run + test (duckdb) ==" -ForegroundColor Cyan

# --- robust python/dbt detection ---
# Goal: run dbt reliably on Windows + Linux (CI).
# Windows: prefer dbt.exe next to python.exe in venv Scripts/
# Linux/macOS: prefer dbt found on PATH (venv bin/) or next to python
$dbt = $null

try {
    # 1) Windows venv layout: dbt.exe in same folder as python.exe
    if ($PythonExe -and (Test-Path $PythonExe)) {
        $pyDir = Split-Path $PythonExe -Parent
        $dbtWin = Join-Path $pyDir "dbt.exe"
        if (Test-Path $dbtWin) { $dbt = $dbtWin }
    }

    # 2) Cross-platform: dbt on PATH (e.g., venv bin/dbt on Linux)
    if (-not $dbt) {
        $cmd = Get-Command dbt -ErrorAction SilentlyContinue
        if ($cmd) { $dbt = $cmd.Source }
    }

    if (-not $dbt) {
        throw "dbt executable not found. Ensure dbt-duckdb is installed in the active environment."
    }
}
catch {
    throw param(
  [switch]$Force,
  [string]$PythonExe = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-PythonExe {
  param([string]$Explicit)

  # If user provided something, accept either:
  #  - an existing file path
  #  - a command name resolvable via Get-Command (python/python3/etc.)
  if ($Explicit) {
    if (Test-Path $Explicit) {
      return [System.IO.Path]::GetFullPath($Explicit)
    }
    try {
      return (Get-Command $Explicit -ErrorAction Stop).Source
    } catch {
      $p = [System.IO.Path]::GetFullPath($Explicit)
      throw "PythonExe not found as path or command: '$Explicit' (resolved path tried: $p)"
    }
  }

  # Prefer active venv if set (Windows + Linux)
  if ($env:VIRTUAL_ENV) {
    $cands = @(
      (Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"),
      (Join-Path $env:VIRTUAL_ENV "Scripts\python"),
      (Join-Path $env:VIRTUAL_ENV "bin/python"),
      (Join-Path $env:VIRTUAL_ENV "bin/python3")
    )
    foreach ($c in $cands) { if (Test-Path $c) { return $c } }
  }

  # Prefer repo .venv (Windows + Linux)
  $repo = (Get-Location).Path
  $cands2 = @(
    (Join-Path $repo ".venv\Scripts\python.exe"),
    (Join-Path $repo ".venv\Scripts\python"),
    (Join-Path $repo ".venv\bin\python"),
    (Join-Path $repo ".venv\bin\python3")
  )
  foreach ($c in $cands2) { if (Test-Path $c) { return $c } }

  # Fallback: python3 then python
  foreach ($name in @("python3", "python")) {
    try { return (Get-Command $name -ErrorAction Stop).Source } catch { }
  }

  throw "Could not resolve a Python executable (tried venv + .venv + python3/python)."
}

function Invoke-Py {
  param(
    [Parameter(Mandatory=$true)][string]$Py,
    [Parameter(Mandatory=$true)][string[]]$Args
  )

  $cmd = "`"$Py`" " + ($Args -join " ")
  Write-Host $cmd -ForegroundColor DarkGray

  & $Py @Args
  if ($LASTEXITCODE -ne 0) {
    throw "Python command failed (exit=$LASTEXITCODE): $cmd"
  }
}

$py = Resolve-PythonExe -Explicit $PythonExe

Write-Host "== Retail Ops M5: RUN ==" -ForegroundColor Cyan
Write-Host ("python = " + $py) -ForegroundColor Gray

Write-Host "`n== ensure-dirs ==" -ForegroundColor Cyan
Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","ensure-dirs")

Write-Host "`n== train-m5 ==" -ForegroundColor Cyan
if ($Force) {
  Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","train-m5","--force")
} else {
  Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","train-m5")
}

Write-Host "`n== eval-m5 ==" -ForegroundColor Cyan
if ($Force) {
  Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","eval-m5","--force")
} else {
  Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","eval-m5")
}

Write-Host "`n== sanity: required artifacts exist ==" -ForegroundColor Cyan
$need = @(
  ".\outputs\models\m5_ridge_baseline.joblib",
  ".\outputs\reports\train_m5.json",
  ".\outputs\reports\eval_m5.json",
  ".\outputs\tables\eval_m5_metrics.csv",
  ".\outputs\tables\eval_m5_predictions.csv",
  ".\outputs\figures\eval_m5_pred_vs_true.pdf",
  ".\outputs\figures\eval_m5_residuals.pdf"
)

foreach ($p in $need) {
  if (-not (Test-Path $p)) { throw "Missing artifact: $p" }
}

Write-Host "`n== sanity: metrics preview ==" -ForegroundColor Cyan
Get-Content ".\outputs\tables\eval_m5_metrics.csv" -Raw

Write-Host "`nOK: run completed" -ForegroundColor Green

Write-Host ""
Write-Host "== dbt: run + test (duckdb) ==" -ForegroundColor Cyan

# --- robust python/dbt detection ---
$pyForDbt = $null
if ($PSBoundParameters.ContainsKey("PythonExe") -and $PythonExe) { $pyForDbt = $PythonExe }
elseif ($PythonExe) { $pyForDbt = $PythonExe }
elseif (Test-Path ".\.venv\Scripts\python.exe") { $pyForDbt = (Resolve-Path ".\.venv\Scripts\python.exe").Path }
elseif (Get-Command python -ErrorAction SilentlyContinue) { $pyForDbt = "python" }
else { throw "Could not determine python executable for dbt." }

$pyDir = Split-Path $pyForDbt -Parent
$dbt = Join-Path $pyDir "dbt.exe"
if (-not (Test-Path $dbt)) {
    $dbtAlt = Join-Path $pyDir "dbt"
    if (Test-Path $dbtAlt) { $dbt = $dbtAlt }
    else { throw "dbt executable not found next to python. Expected $dbt (or dbt)." }
}
# --- end robust python/dbt detection ---
if (-not (Test-Path $dbt)) {
    throw "dbt.exe not found next to PythonExe. Expected: $dbt"
}

# Ensure dbt outputs dir exists (duckdb file path lives under outputs/dbt)
New-Item -ItemType Directory -Force -Path ".\outputs\dbt" | Out-Null

& $dbt run  --project-dir .\dbt\m5 --profiles-dir .\dbt
if ($LASTEXITCODE -ne 0) { throw "dbt run failed (exit=$LASTEXITCODE)" }

& $dbt test --project-dir .\dbt\m5 --profiles-dir .\dbt
if ($LASTEXITCODE -ne 0) { throw "dbt test failed (exit=$LASTEXITCODE)" }

}
# --- end robust python/dbt detection ---
if (-not (Test-Path $dbt)) {
    throw "dbt.exe not found next to PythonExe. Expected: $dbt"
}

# Ensure dbt outputs dir exists (duckdb file path lives under outputs/dbt)
New-Item -ItemType Directory -Force -Path ".\outputs\dbt" | Out-Null

& $dbt run  --project-dir .\dbt\m5 --profiles-dir .\dbt
if ($LASTEXITCODE -ne 0) { throw "dbt run failed (exit=$LASTEXITCODE)" }

& $dbt test --project-dir .\dbt\m5 --profiles-dir .\dbt
if ($LASTEXITCODE -ne 0) { throw "dbt test failed (exit=$LASTEXITCODE)" }
