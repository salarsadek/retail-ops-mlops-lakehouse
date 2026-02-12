param(
  [switch]$Force,
  [string]$PythonExe = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-PythonExe {
  param([string]$Explicit)

  if ($Explicit) {
    $p = [System.IO.Path]::GetFullPath($Explicit)
    if (-not (Test-Path $p)) { throw "PythonExe not found: $p" }
    return $p
  }

  # 1) If venv is active, prefer that
  if ($env:VIRTUAL_ENV) {
    $cand = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
    if (Test-Path $cand) { return $cand }
  }

  # 2) If repo has .venv, prefer that
  $repo = (Get-Location).Path
  $cand2 = Join-Path $repo ".venv\Scripts\python.exe"
  if (Test-Path $cand2) { return $cand2 }

  # 3) Fallback to whatever "python" is
  return (Get-Command python).Source
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
