param(
  [Parameter(Mandatory=$true)]
  [ValidateSet("qa","test","format","lint")]
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
}
