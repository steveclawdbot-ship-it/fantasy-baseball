#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$ROOT/.logs"

for svc in backend frontend; do
  PID_FILE="$LOG_DIR/$svc.pid"
  if [[ -f "$PID_FILE" ]]; then
    PID="$(cat "$PID_FILE")"
    if kill -0 "$PID" 2>/dev/null; then
      echo "[stop] $svc ($PID)"
      kill "$PID" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
done

echo "done"
