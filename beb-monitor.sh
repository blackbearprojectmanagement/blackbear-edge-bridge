#!/usr/bin/env bash

set -uo pipefail

HEALTH_URL="${BEB_HEALTH_URL:-http://127.0.0.1:8000/health}"
PROJECT_DIR="${BEB_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" 2>/dev/null && pwd)}"
DB_PATH_IN_CONTAINER="${BEB_DB_PATH_IN_CONTAINER:-/app/data/bridge.db}"
MOSQUITTO_LOG_TAIL="${BEB_MOSQUITTO_LOG_TAIL:-500}"

section() {
  printf '\n== %s ==\n' "$1"
}

run_compose() {
  if ! command -v docker >/dev/null 2>&1; then
    printf 'Docker is unavailable on PATH.\n'
    return 127
  fi
  docker compose "$@"
}

print_health_time_summary() {
  if [ -z "${HEALTH_JSON:-}" ]; then
    printf 'Health JSON unavailable; timestamp summary skipped.\n'
    return
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    printf 'python3 unavailable; timestamp summary skipped.\n'
    return
  fi

  HEALTH_JSON="$HEALTH_JSON" python3 - <<'PY'
import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

fields = [
    ("worker last heartbeat", "worker_last_heartbeat"),
    ("readiness last check", "beb_ready_last_check_at"),
    ("readiness last success", "beb_ready_last_success_at"),
    ("readiness last failure", "beb_ready_last_failure_at"),
    ("readiness last publication", "beb_ready_last_published_at"),
]

try:
    payload = json.loads(os.environ["HEALTH_JSON"])
except Exception as exc:
    print(f"Malformed health JSON; timestamp summary skipped: {exc}")
    raise SystemExit(0)

ist = ZoneInfo("Asia/Kolkata")
for label, key in fields:
    value = payload.get(key)
    if not value:
        print(f"{label}: unavailable")
        continue
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        print(f"{label}: {parsed.astimezone(ist).isoformat(timespec='seconds')}")
    except Exception as exc:
        print(f"{label}: malformed ({value}) - {exc}")
PY
}

print_sqlite_summary() {
  if ! command -v docker >/dev/null 2>&1; then
    printf 'Docker is unavailable; SQLite checks skipped.\n'
    return
  fi

BEB_DB_PATH_IN_CONTAINER="$DB_PATH_IN_CONTAINER" docker compose exec -T beb python - <<'PY'
import os
from pathlib import Path

try:
    import sqlite3
except Exception as exc:
    print(f"sqlite3 unavailable in BEB container Python: {exc}")
    raise SystemExit(0)

db_path = Path(os.environ.get("BEB_DB_PATH_IN_CONTAINER", "/app/data/bridge.db"))
if not db_path.exists():
    print(f"Database missing: {db_path}")
    raise SystemExit(0)

try:
    connection = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
except Exception as exc:
    print(f"Unable to open SQLite database read-only: {exc}")
    raise SystemExit(0)

connection.row_factory = sqlite3.Row

def table_exists(name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None

try:
    print("SQLite integrity:")
    try:
        for row in connection.execute("PRAGMA integrity_check;"):
            print(f"  {row[0]}")
    except Exception as exc:
        print(f"  integrity_check unavailable: {exc}")

    print("\nmqtt_messages status summary:")
    if table_exists("mqtt_messages"):
        for row in connection.execute(
            "SELECT status, COUNT(*) AS count FROM mqtt_messages GROUP BY status ORDER BY status"
        ):
            print(f"  {row['status']}: {row['count']}")
    else:
        print("  mqtt_messages table missing")

    print("\napi_commands status summary:")
    if table_exists("api_commands"):
        for row in connection.execute(
            "SELECT status, COUNT(*) AS count FROM api_commands GROUP BY status ORDER BY status"
        ):
            print(f"  {row['status']}: {row['count']}")
    else:
        print("  api_commands table missing")

    print("\nLatest 10 mqtt_messages:")
    if table_exists("mqtt_messages"):
        columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(mqtt_messages)")
        }
        required = {"id", "received_at", "table_no", "model", "serial", "status"}
        missing = sorted(required - columns)
        if missing:
            print(f"  required columns missing: {', '.join(missing)}")
        else:
            print("  id | received_at | table_no | model | serial | status")
            for row in connection.execute(
                """
                SELECT id, received_at, table_no, model, serial, status
                FROM mqtt_messages
                ORDER BY id DESC
                LIMIT 10
                """
            ):
                print(
                    "  {id} | {received_at} | {table_no} | {model} | {serial} | {status}".format(
                        **dict(row)
                    )
                )
    else:
        print("  mqtt_messages table missing")
finally:
    connection.close()
PY
}

printf 'BLACKBEAR INDUSTRY 4.0 - BBW LIVE MONITOR\n'
printf 'Host time: %s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')"

if [ ! -d "$PROJECT_DIR" ]; then
  printf 'Project directory missing: %s\n' "$PROJECT_DIR"
  exit 1
fi

cd "$PROJECT_DIR" || {
  printf 'Unable to enter project directory: %s\n' "$PROJECT_DIR"
  exit 1
}

section "Docker Compose"
run_compose ps || printf 'docker compose ps failed.\n'

section "BEB Health"
HEALTH_JSON=""
if command -v curl >/dev/null 2>&1; then
  HEALTH_OUTPUT="$(curl -fsS --max-time 5 "$HEALTH_URL" 2>&1)"
  HEALTH_RC=$?
  if [ "$HEALTH_RC" -eq 0 ]; then
    HEALTH_JSON="$HEALTH_OUTPUT"
    printf '%s\n' "$HEALTH_JSON"
  else
    printf 'Health endpoint unreachable: %s\n' "$HEALTH_OUTPUT"
  fi
else
  printf 'curl unavailable; health endpoint skipped.\n'
fi

section "Application Timestamps In IST"
print_health_time_summary

section "Docker Resources"
run_compose ps >/dev/null 2>&1
if [ "$?" -eq 0 ]; then
  docker stats --no-stream --format 'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}\t{{.BlockIO}}' beb beb-mosquitto || \
    printf 'docker stats failed.\n'
else
  printf 'Docker Compose stack unavailable; resource check skipped.\n'
fi

section "MQTT Client History"
if run_compose logs --tail="$MOSQUITTO_LOG_TAIL" mosquitto 2>/dev/null | grep -E 'BEB_BBW_TABLE1|FX5U_TEST'; then
  :
else
  printf 'No filtered MQTT client history found for BEB_BBW_TABLE1 or FX5U_TEST.\n'
fi

section "SQLite Read-Only Checks"
print_sqlite_summary

section "Continuous BEB Logs"
if command -v stdbuf >/dev/null 2>&1; then
  stdbuf -oL -eL docker compose logs -f beb
else
  docker compose logs -f beb
fi
