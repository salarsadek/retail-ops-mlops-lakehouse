param(
  [Parameter(Mandatory=$true)]
  [ValidateSet("qa","test","format","lint","cfg","ensure-dirs")]
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
}
