#!/usr/bin/env bash
set -euo pipefail

# Fantasy Baseball API startup (stable local runtime)
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$ROOT/.venv"

if [[ ! -x "$VENV/bin/python" ]]; then
  echo "[setup] creating venv at $VENV"
  python3 -m venv "$VENV"
fi

echo "[setup] installing backend deps"
"$VENV/bin/python" -m pip install -q --upgrade pip
"$VENV/bin/python" -m pip install -q -r "$ROOT/backend/requirements.txt"

export FANTASY_DB_PATH="${FANTASY_DB_PATH:-/home/jesse/clawd-steve/data/fantasy_baseball.db}"

echo "[run] fantasy api on http://0.0.0.0:8000"
cd "$ROOT/backend"
exec "$VENV/bin/python" -m uvicorn app.main:app --host 0.0.0.0 --port 8000
