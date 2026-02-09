#!/usr/bin/env bash
set -euo pipefail

task="${1:-}"

if [[ -z "$task" ]]; then
  echo "Usage: ./scripts/run.sh {qa|test|format|lint}"
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
  *)
    echo "Unknown task: $task"
    exit 2
    ;;
esac
