param(
  [switch]$Force,
  [string]$PythonExe = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-PythonExe {
  param([string]$Explicit)

  if ($Explicit) {
    if (Test-Path $Explicit) { return [System.IO.Path]::GetFullPath($Explicit) }
    try { return (Get-Command $Explicit -ErrorAction Stop).Source } catch {
      $p = [System.IO.Path]::GetFullPath($Explicit)
      throw "PythonExe not found as path or command: '$Explicit' (resolved path tried: $p)"
    }
  }

  if ($env:VIRTUAL_ENV) {
    $cands = @(
      (Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"),
      (Join-Path $env:VIRTUAL_ENV "Scripts\python"),
      (Join-Path $env:VIRTUAL_ENV "bin/python"),
      (Join-Path $env:VIRTUAL_ENV "bin/python3")
    )
    foreach ($c in $cands) { if (Test-Path $c) { return $c } }
  }

  $repo = (Get-Location).Path
  $cands2 = @(
    (Join-Path $repo ".venv\Scripts\python.exe"),
    (Join-Path $repo ".venv\Scripts\python"),
    (Join-Path $repo ".venv\bin\python"),
    (Join-Path $repo ".venv\bin\python3")
  )
  foreach ($c in $cands2) { if (Test-Path $c) { return $c } }

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
  if ($LASTEXITCODE -ne 0) { throw "Python command failed (exit=$LASTEXITCODE): $cmd" }
}

function Resolve-DbtExe {
  param([string]$Py)

  # 1) Prefer dbt next to python (Windows venv Scripts/ or Linux venv bin/)
  try {
    $pyDir = Split-Path $Py -Parent
    if ($pyDir -and (Test-Path $pyDir)) {
      $candWin = Join-Path $pyDir "dbt.exe"
      if (Test-Path $candWin) { return $candWin }

      $candNix = Join-Path $pyDir "dbt"
      if (Test-Path $candNix) { return $candNix }
    }
  } catch { }

  # 2) Fallback: dbt on PATH (works in CI ubuntu when installed)
  $cmd = Get-Command dbt -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }

  throw "dbt executable not found. Ensure dbt-duckdb is installed in the active environment."
}

$py = Resolve-PythonExe -Explicit $PythonExe

Write-Host "== Retail Ops M5: RUN ==" -ForegroundColor Cyan
Write-Host ("python = " + $py) -ForegroundColor Gray

Write-Host "`n== ensure-dirs ==" -ForegroundColor Cyan
Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","ensure-dirs")

Write-Host "`n== train-m5 ==" -ForegroundColor Cyan
if ($Force) { Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","train-m5","--force") }
else       { Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","train-m5") }

Write-Host "`n== eval-m5 ==" -ForegroundColor Cyan
if ($Force) { Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","eval-m5","--force") }
else       { Invoke-Py -Py $py -Args @("-m","retail_ops_mlops.cli","eval-m5") }

Write-Host "`n== sanity: required artifacts exist ==" -ForegroundColor Cyan
$required = @(
  "outputs/reports/train_m5.json",
  "outputs/reports/eval_m5.json"
)
foreach ($r in $required) {
  if (-not (Test-Path $r)) { throw "Missing required artifact: $r" }
}

Write-Host "`n== sanity: metrics preview ==" -ForegroundColor Cyan
$metrics = "outputs/tables/metrics_m5_latest.csv"
if (Test-Path $metrics) {
  Get-Content $metrics | Select-Object -First 6
}

Write-Host "`nOK: run completed" -ForegroundColor Green

Write-Host ""
Write-Host "== dbt: run + test (duckdb) ==" -ForegroundColor Cyan

# Ensure dbt output dir exists for duckdb file
New-Item -ItemType Directory -Force -Path ".\outputs\dbt" | Out-Null

$dbt = Resolve-DbtExe -Py $py
Write-Host ("dbt = " + $dbt) -ForegroundColor Gray

# Run dbt (project under dbt/m5, profiles under dbt/)
& $dbt run --profiles-dir ".\dbt" --project-dir ".\dbt\m5"
if ($LASTEXITCODE -ne 0) { throw "dbt run failed (exit=$LASTEXITCODE)" }

& $dbt test --profiles-dir ".\dbt" --project-dir ".\dbt\m5"
if ($LASTEXITCODE -ne 0) { throw "dbt test failed (exit=$LASTEXITCODE)" }
