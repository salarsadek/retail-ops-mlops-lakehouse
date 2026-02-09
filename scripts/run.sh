#!/usr/bin/env bash
set -euo pipefail

task="${1:-}"

if [[ -z "$task" ]]; then
  echo "Usage: ./scripts/run.sh {qa|test|format|lint|cfg|ensure-dirs|ingest-m5|ingest-m5-dry}"
  exit 2
fi

case "$task" in
  format)
    ruff format .
    ;;
  lint)
    ruff check .
    ;;
  test)
    pytest -q
    ;;
  qa)
    ruff format .
    ruff check .
    pytest -q
    ;;
  cfg)
    python -m retail_ops_mlops show-paths
    ;;
  ensure-dirs)
    python -m retail_ops_mlops ensure-dirs
    ;;
  ingest-m5-dry)
    python -m retail_ops_mlops ingest-m5 --no-strict
    ;;
  ingest-m5)
    python -m retail_ops_mlops ingest-m5
    ;;
  *)
    echo "Unknown task: $task"
    exit 2
    ;;
esac
