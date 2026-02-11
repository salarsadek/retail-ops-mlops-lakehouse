param(
  [Parameter(Position = 0)]
  [string]$Task = "help",

  [string]$Config = "configs/default.yaml",

  [string]$ZipPath = "",

  [switch]$Force,
  [switch]$NoStrict
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$strictFlag = if ($NoStrict) { "--no-strict" } else { "--strict" }
$forceFlag  = if ($Force)    { "--force" }     else { "" }

function Invoke-Cli {
  param([Parameter(Mandatory=$true)][string[]]$Args)

  $cmd = @("python", "-m", "retail_ops_mlops") + $Args
  Write-Host ("
>> " + ($cmd -join " ")) -ForegroundColor Cyan
  & $cmd[0] $cmd[1..($cmd.Length-1)]
}

switch ($Task) {
  "help" {
    Write-Host "Tasks:" -ForegroundColor Yellow
    @(
      "help",
      "lint",
      "test",
      "show-paths",
      "ensure-dirs",
      "download-m5",
      "ingest-m5",
      "bronze-m5",
      "silver-m5",
      "gold-m5",
      "dq-m5",
      "run-m5",
      "all-m5"
    ) | ForEach-Object { "  - $_" } | Write-Host

    Write-Host "
Examples:" -ForegroundColor Yellow
    Write-Host "  .\scripts\run.ps1 run-m5"
    Write-Host "  .\scripts\run.ps1 dq-m5"
    Write-Host "  .\scripts\run.ps1 all-m5"
    Write-Host "  .\scripts\run.ps1 ingest-m5 -ZipPath C:\path\m5.zip"
    break
  }

  "lint" {
    Write-Host "
>> ruff format src" -ForegroundColor Cyan
    ruff format src
    Write-Host "
>> ruff check src" -ForegroundColor Cyan
    ruff check src
    Write-Host "
>> pre-commit run -a" -ForegroundColor Cyan
    pre-commit run -a
    break
  }

  "test" {
    Write-Host "
>> pytest -q" -ForegroundColor Cyan
    pytest -q
    break
  }

  "show-paths"  { Invoke-Cli @("show-paths", "--config", $Config); break }
  "ensure-dirs" { Invoke-Cli @("ensure-dirs", "--config", $Config); break }

  "download-m5" {
    Invoke-Cli @("download-m5", "--config", $Config, $forceFlag, $strictFlag) | Where-Object { $_ -ne "" }
    break
  }

  "ingest-m5" {
    $args = @("ingest-m5", "--config", $Config, $strictFlag)
    if ($ZipPath -ne "") { $args += @("--zip-path", $ZipPath) }
    Invoke-Cli $args
    break
  }

  "bronze-m5" { Invoke-Cli @("bronze-m5", "--config", $Config, $forceFlag, $strictFlag) | Where-Object { $_ -ne "" }; break }
  "silver-m5" { Invoke-Cli @("silver-m5", "--config", $Config, $forceFlag, $strictFlag) | Where-Object { $_ -ne "" }; break }
  "gold-m5"   { Invoke-Cli @("gold-m5",   "--config", $Config, $forceFlag, $strictFlag) | Where-Object { $_ -ne "" }; break }
  "dq-m5"     { Invoke-Cli @("dq-m5",     "--config", $Config, $strictFlag) | Where-Object { $_ -ne "" }; break }

  "run-m5" {
    $args = @("run-m5", "--config", $Config, $forceFlag, $strictFlag)
    if ($ZipPath -ne "") { $args += @("--zip-path", $ZipPath) }
    Invoke-Cli $args
    break
  }

  "all-m5" {
    Invoke-Cli @("run-m5", "--config", $Config, $forceFlag, $strictFlag)
    Invoke-Cli @("dq-m5",  "--config", $Config, $strictFlag)
    break
  }

  default {
    throw "Unknown task '$Task'. Run: .\scripts\run.ps1 help"
  }
}
