#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
VENV="$ROOT/.venv"
LOG_DIR="$ROOT/.logs"
mkdir -p "$LOG_DIR"

API_HOST="${API_HOST:-127.0.0.1}"
API_PORT="${API_PORT:-8000}"
WEB_HOST="${WEB_HOST:-127.0.0.1}"
WEB_PORT="${WEB_PORT:-4173}"

port_in_use() {
  local host="$1"; local port="$2"
  python3 - <<'PY' "$host" "$port"
import socket,sys
h=sys.argv[1]; p=int(sys.argv[2])
s=socket.socket(); s.settimeout(0.2)
try:
    s.connect((h,p)); print('1')
except Exception:
    print('0')
finally:
    s.close()
PY
}

if [[ "$(port_in_use "$API_HOST" "$API_PORT")" == "1" ]]; then
  echo "[warn] api port $API_PORT already in use, falling back to 8002"
  API_PORT=8002
fi
if [[ "$(port_in_use "$WEB_HOST" "$WEB_PORT")" == "1" ]]; then
  echo "[warn] web port $WEB_PORT already in use, falling back to 4174"
  WEB_PORT=4174
fi

if [[ ! -x "$VENV/bin/python" ]]; then
  echo "[setup] creating venv at $VENV"
  python3 -m venv "$VENV"
fi

"$VENV/bin/python" -m pip install -q --upgrade pip
"$VENV/bin/python" -m pip install -q -r "$ROOT/backend/requirements.txt"

export FANTASY_DB_PATH="${FANTASY_DB_PATH:-/home/jesse/clawd-steve/data/fantasy_baseball.db}"

echo "[run] starting backend on http://$API_HOST:$API_PORT"
(
  cd "$ROOT/backend"
  exec "$VENV/bin/python" -m uvicorn app.main:app --host "$API_HOST" --port "$API_PORT"
) > "$LOG_DIR/backend.log" 2>&1 &
API_PID=$!

echo "$API_PID" > "$LOG_DIR/backend.pid"

cleanup() {
  echo "[stop] shutting down services"
  [[ -n "${API_PID:-}" ]] && kill "$API_PID" 2>/dev/null || true
  [[ -n "${WEB_PID:-}" ]] && kill "$WEB_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# quick backend health wait
for i in {1..20}; do
  if curl -fsS "http://$API_HOST:$API_PORT/api/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

if ! curl -fsS "http://$API_HOST:$API_PORT/api/health" >/dev/null 2>&1; then
  echo "[error] backend failed healthcheck. see $LOG_DIR/backend.log"
  exit 1
fi

echo "[run] starting frontend on http://$WEB_HOST:$WEB_PORT"
(
  cd "$ROOT/frontend"
  exec "$VENV/bin/python" -m http.server "$WEB_PORT" --bind "$WEB_HOST"
) > "$LOG_DIR/frontend.log" 2>&1 &
WEB_PID=$!
echo "$WEB_PID" > "$LOG_DIR/frontend.pid"

echo ""
echo "✅ fantasy app is live"
echo "- frontend: http://$WEB_HOST:$WEB_PORT"
echo "- backend:  http://$API_HOST:$API_PORT"
echo "- api docs: http://$API_HOST:$API_PORT/docs"
echo ""
echo "logs: $LOG_DIR/backend.log | $LOG_DIR/frontend.log"
echo "press ctrl+c to stop"

wait "$API_PID" "$WEB_PID"
