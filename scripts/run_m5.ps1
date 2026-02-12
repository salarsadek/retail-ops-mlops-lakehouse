param(
  [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Py {
  param([Parameter(Mandatory=$true)][string[]]$Args)
  $cmd = "python " + ($Args -join " ")
  Write-Host $cmd -ForegroundColor DarkGray
  & python @Args
  if ($LASTEXITCODE -ne 0) { throw "Python command failed (exit=$LASTEXITCODE): $cmd" }
}

Write-Host "== Retail Ops M5: RUN ==" -ForegroundColor Cyan
Write-Host ("python = " + (Get-Command python).Source) -ForegroundColor Gray

Write-Host "`n== ensure-dirs ==" -ForegroundColor Cyan
Invoke-Py @("-m","retail_ops_mlops.cli","ensure-dirs")

Write-Host "`n== build-features-m5 ==" -ForegroundColor Cyan
if ($Force) { Invoke-Py @("-m","retail_ops_mlops.cli","build-features-m5","--force") }
else { Invoke-Py @("-m","retail_ops_mlops.cli","build-features-m5") }

Write-Host "`n== dq-m5 (quality gate) ==" -ForegroundColor Cyan
if ($Force) { Invoke-Py @("-m","retail_ops_mlops.cli","dq-m5","--force") }
else { Invoke-Py @("-m","retail_ops_mlops.cli","dq-m5") }

Write-Host "`n== train-m5 ==" -ForegroundColor Cyan
if ($Force) { Invoke-Py @("-m","retail_ops_mlops.cli","train-m5","--force") }
else { Invoke-Py @("-m","retail_ops_mlops.cli","train-m5") }

Write-Host "`n== eval-m5 ==" -ForegroundColor Cyan
if ($Force) { Invoke-Py @("-m","retail_ops_mlops.cli","eval-m5","--force") }
else { Invoke-Py @("-m","retail_ops_mlops.cli","eval-m5") }

Write-Host "`n== sanity: required artifacts exist ==" -ForegroundColor Cyan
$need = @(
  ".\data\processed\m5\gold\fact_sales_features_sample.parquet",
  ".\outputs\reports\build_features_m5.json",
  ".\outputs\reports\dq_m5.json",
  ".\outputs\models\m5_ridge_baseline.joblib",
  ".\outputs\reports\train_m5.json",
  ".\outputs\reports\eval_m5.json",
  ".\outputs\tables\eval_m5_metrics.csv",
  ".\outputs\tables\eval_m5_predictions.csv",
  ".\outputs\figures\eval_m5_pred_vs_true.pdf",
  ".\outputs\figures\eval_m5_residuals.pdf"
)

foreach ($p in $need) { if (-not (Test-Path $p)) { throw "Missing artifact: $p" } }

Write-Host "`n== sanity: metrics preview ==" -ForegroundColor Cyan
Get-Content ".\outputs\tables\eval_m5_metrics.csv" -Raw

Write-Host "`nOK: run completed" -ForegroundColor Green
