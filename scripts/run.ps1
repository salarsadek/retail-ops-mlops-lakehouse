param(
  [Parameter(Mandatory=$true)]
  [ValidateSet("qa","test","format","lint","cfg","ensure-dirs","ingest-m5","ingest-m5-dry")]
  [string]$Task
)

$ErrorActionPreference = "Stop"

function Info($msg) { Write-Host "[run.ps1] $msg" }

switch ($Task) {
  "format" {
    Info "Formatting with ruff..."
    ruff format .
  }
  "lint" {
    Info "Linting with ruff..."
    ruff check .
  }
  "test" {
    Info "Running tests..."
    pytest -q
  }
  "qa" {
    Info "Running full quality gate: format + lint + tests"
    ruff format .
    ruff check .
    pytest -q
  }
  "cfg" {
    Info "Showing resolved config paths..."
    python -m retail_ops_mlops show-paths
  }
  "ensure-dirs" {
    Info "Ensuring directories exist..."
    python -m retail_ops_mlops ensure-dirs
  }
  "ingest-m5-dry" {
    Info "Running M5 ingest (non-strict) to produce a report even if zip is missing..."
    python -m retail_ops_mlops ingest-m5 --no-strict
  }
  "ingest-m5" {
    Info "Running M5 ingest (strict) ..."
    python -m retail_ops_mlops ingest-m5
  }
}
