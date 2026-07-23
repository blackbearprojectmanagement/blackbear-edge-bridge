# BlackBear Edge Bridge (BEB)

BlackBear Edge Bridge is a Python service that bridges AWS-hosted Odoo, a local Mosquitto broker, and a PLC. It exposes a small authenticated HTTPS-ready API for Odoo-to-PLC commands, listens for PLC MQTT messages, stores PLC-to-Odoo events in a local SQLite queue, then optionally forwards queued messages to Odoo through XML-RPC.

BEB does not print. Odoo continues to control the existing CUPS printing workflow, label templates, print routing, and printer communication.

## Architecture

```text
AWS-hosted Odoo
 |
 | HTTPS POST /api/v1/plc/command
 v
Secure public BEB API URL
 |
 v
BlackBear Edge Bridge
 |
 | MQTT publish
 v
Local Mosquitto
 |
 v
PLC
```

Return path:

```text
PLC
 |
 | MQTT publish
 v
Local Mosquitto
 |
 v
BlackBear Edge Bridge
 |
 v
SQLite Queue
 |
 v
Odoo XML-RPC API
 |
 v
Existing Odoo CUPS Printing Workflow
```

There is no Odoo command polling. Odoo pushes commands to BEB immediately through the authenticated API. PLC-to-Odoo events continue to use the SQLite-backed XML-RPC queue.

Confirmed Odoo test integration:

```text
URL      : https://test-bbw.odoo.com
Database : broadtechit-test-bbw-stage-34933250
Model    : iot.configuration
Method   : xmlrpc_submit_print_data
```

## Message Flow

```text
MQTT receive
 |
 v
Decode UTF-8 JSON
 |
 v
Parse MN / MP payload
 |
 v
Store valid message in SQLite as NEW
 |
 v
Background worker claims NEW / retryable FAILED rows
 |
 v
Submit original raw JSON payload to Odoo XML-RPC
 |
 v
Mark COMPLETED or FAILED
```

The MQTT callback never calls Odoo directly.

## MQTT

BEB uses MQTT v3.1.1, paho-mqtt Callback API Version 2, and QoS 0.

```text
PLC to BEB : MQTT/PLC_TO_ODOO/topic
BEB to PLC : MQTT/ODOO_TO_PLC/topic
```

BEB publishes Odoo-to-PLC API commands to the PLC command topic with QoS 0 and retain disabled.

BEB can also publish its own Odoo-path readiness status to the PLC-bound topic configured by `BEB_READY_TOPIC`, which defaults to the existing `MQTT/ODOO_TO_PLC/topic`. The readiness payload is compact JSON and contains only the `BR` key:

```json
{"BR":1}
{"BR":0}
```

`{"BR":1}` means BEB has confirmed that the Odoo XML-RPC path is reachable and authenticating. `{"BR":0}` means BEB cannot currently confirm that path. This is an operational readiness signal for PLC logic such as M200; it is not a safety interlock. BEB publishes `BR` only on confirmed state changes, never as a continuous heartbeat.

At startup the readiness state is unknown/not ready. BEB publishes `BR=1` only after 10 seconds of continuous successful Odoo checks. If the first check fails, BEB publishes `BR=0` once. From READY, failures must continue for 5 seconds before BEB publishes `BR=0`; from NOT_READY, successes must continue for 10 seconds before BEB publishes `BR=1`. Brief opposite results reset the active debounce timer. The readiness loop runs every `BEB_READY_CHECK_INTERVAL_SECONDS`, default `30` seconds. Each readiness check uses the short `BEB_READY_CHECK_TIMEOUT_SECONDS` timeout, default `3` seconds, and reuses a valid Odoo UID instead of authenticating on every healthy check. BEB re-authenticates when authentication is absent, invalidated after an authentication-related failure, or older than `BEB_READY_AUTH_REVALIDATE_SECONDS`, default `300` seconds. Readiness does not inherit the production print/inventory `ODOO_TIMEOUT=90`.

Supported PLC payloads:

```json
{"MN":"106-020C012P001 3242T01"}
{"MP":"Z106-015C020P001 7084T01"}
```

`MN` means Print Completed. `MP` means Loose Packet.

Supported Odoo-to-PLC API command payloads:

```json
{"messt01": "Z106-020C012P001"}
{"messt02": "Z106-020C012P001"}
{"messt03": "Z106-020C012P001"}
{"T01": "P"}
{"T01": "R"}
{"T01": "D"}
{"T02": "P"}
{"T02": "R"}
{"T02": "D"}
{"T03": "P"}
{"T03": "R"}
{"T03": "D"}
{"LP": "FT01"}
{"LP": "FT02"}
{"LP": "FT03"}
```

## SQLite Queue

Database path:

```text
data/bridge.db
```

The database is created automatically on first run. Existing data is preserved during migrations.

SQLite timestamps are stored in UTC ISO-8601 form. Keep the application and API timestamp policy UTC; human-facing conversion, such as IST display, belongs in monitoring or dashboard layers.

Table: `mqtt_messages`

```text
id INTEGER PRIMARY KEY AUTOINCREMENT
received_at TEXT NOT NULL
topic TEXT NOT NULL
message_type TEXT NOT NULL
table_no TEXT NOT NULL
model TEXT NOT NULL
serial TEXT NOT NULL
raw_payload TEXT NOT NULL
message_hash TEXT NOT NULL UNIQUE
status TEXT NOT NULL
retry_count INTEGER DEFAULT 0
processed_at TEXT
last_error TEXT
odoo_response TEXT
last_attempt_at TEXT
completed_at TEXT
```

Duplicate protection uses SHA256 of:

```text
topic + raw_payload
```

Duplicate MQTT deliveries are ignored and logged as `Duplicate message ignored`.

Table: `api_commands`

```text
id INTEGER PRIMARY KEY AUTOINCREMENT
request_id TEXT NOT NULL UNIQUE
idempotency_key TEXT UNIQUE
received_at TEXT NOT NULL
username TEXT
remote_address TEXT
payload TEXT NOT NULL
payload_hash TEXT NOT NULL
mqtt_topic TEXT NOT NULL
status TEXT NOT NULL
mqtt_rc INTEGER
mqtt_mid INTEGER
published_at TEXT
response_code INTEGER
response_body TEXT
last_error TEXT
```

API command statuses are `RECEIVED`, `PUBLISHED`, `FAILED`, `DUPLICATE`, and `REJECTED`. The audit table is created automatically and does not alter or recreate `mqtt_messages`.

### SQLite Production Summaries

BEB V1 keeps the existing raw queue tables as the source of operational truth and adds dashboard-ready production tables beside them. The raw `mqtt_messages` and `api_commands` tables continue to support queue processing, retries, duplicate protection, Odoo response storage, and API audit history. They are retained for the configured raw retention window.

`production_records` stores recent ACK-level production history for traceability and dashboard recent-activity views. It is created from successful completed MQTT transactions only, preserves the full stored Odoo response in `raw_odoo_response`, and is uniquely keyed by `mqtt_message_id` so retries, duplicate deliveries, process restarts, or ACK replays cannot create multiple production rows for the same source queue row.

`daily_production_summary` stores permanent production counts. It is never removed by raw cleanup. Weekly, monthly, and yearly totals are derived from this daily table with SQL date grouping; BEB does not maintain separate weekly, monthly, or yearly tables.

Daily summary aggregation key:

```text
production_date
machine_id
table_no
model
customer_id
batch_number
operator_id
```

SQLite treats `NULL` values as distinct in unique indexes, so BEB uses a unique expression index with stable sentinel values for nullable `customer_id`, `batch_number`, and `operator_id`. This preserves one summary row per logical dashboard filter combination even when optional metadata is absent.

On successful Odoo completion, BEB stores the full Odoo response in `mqtt_messages.odoo_response`, marks the row `COMPLETED`, upserts exactly one `production_records` row, increments `daily_production_summary`, marks `production_records.summary_applied=1`, and then continues the existing ACK publication path. Invalid or missing optional Odoo metadata is logged and ignored for summary dimensions; it does not block completion or ACK handling.

Startup reconciliation scans a bounded number of completed, ACK-bearing rows that are not yet summarized. It rebuilds production records and daily summaries idempotently from stored SQLite data only. It never contacts Odoo, never republishes ACKs, and never changes completed queue status.

Raw cleanup is configurable and disabled from deleting anything unsafe. By default, records older than 30 days may be removed only when they are old and summarized or otherwise terminal-safe:

- `mqtt_messages`: old `COMPLETED` rows with a summarized `production_records` row.
- `production_records`: old rows with `summary_applied=1`.
- `api_commands`: old terminal `PUBLISHED`, `FAILED`, `REJECTED`, or `DUPLICATE` audit rows.

Cleanup never deletes `NEW`, `PROCESSING`, retryable queue rows, unsummarized production records, rows inside retention, or `daily_production_summary`. Cleanup runs in bounded batches at startup and then once per configured interval; it is not a continuous one-second loop. Zero-delete cycles are DEBUG-only, while deletions and errors are logged at operational levels.

`VACUUM` is disabled by default. If enabled later, treat it as maintenance: it rewrites the SQLite database file and should run only after a verified backup and during an approved service window.

Back up the SQLite volume before first production migration, before changing retention policy, before enabling VACUUM, and before rollback. Schema migration uses `CREATE TABLE IF NOT EXISTS` and additive indexes, so existing production data is upgraded in place without table recreation.

## Status State

```text
NEW -> PROCESSING -> COMPLETED
NEW -> PROCESSING -> FAILED
FAILED -> PROCESSING -> COMPLETED
FAILED -> PROCESSING -> FAILED
```

Retry behavior:

- `FAILED` rows are retried while `retry_count < ODOO_MAX_RETRIES`.
- `mark_failed()` increments `retry_count`.
- `PROCESSING` rows older than `ODOO_STALE_PROCESSING_SECONDS` are recovered as `FAILED` by an independent watchdog.
- Recovered stale rows store `Recovered stale PROCESSING message after timeout` in `last_error`.
- Timed-out `MN`/`MP` print records are terminal `FAILED` manual-verification records. They are not retried automatically and no ACK is published.

## Odoo XML-RPC

When `ODOO_ENABLED=true`, the background worker authenticates with:

```text
https://test-bbw.odoo.com/xmlrpc/2/common
```

It submits queued payloads through:

```text
https://test-bbw.odoo.com/xmlrpc/2/object
```

The object sent to Odoo is loaded from the original stored `raw_payload` and is not reconstructed:

```python
{"MN": "106-020C012P001 3242T01"}
{"MP": "Z106-015C020P001 7084T01"}
```

Any successful XML-RPC return marks the row `COMPLETED`. Odoo print/inventory processing can intermittently take longer than 15 seconds, so the production recommended `ODOO_TIMEOUT` is 90 seconds. Timed-out `MN`/`MP` records are marked `FAILED` with manual verification required, are not retried automatically, and do not publish an ACK. Faults, protocol errors, and connection errors still mark the row `FAILED` and may allow later retry according to the retry policy.

## Authenticated Command API

The API starts only when `BEB_API_ENABLED=true`. The default bind address is `127.0.0.1:8000`; keep it local and expose it externally only through a secure tunnel, VPN, or authenticated reverse proxy.

Health check:

```bash
curl http://127.0.0.1:8000/health
```

The health response preserves the existing fields and also reports worker, readiness, and SQLite lifecycle state. Readiness fields are:

```text
beb_ready_enabled
beb_ready_state
beb_ready_check_timeout_seconds
beb_ready_last_check_at
beb_ready_last_success_at
beb_ready_last_failure_at
beb_ready_last_published_at
beb_ready_last_error
beb_ready_disconnect_elapsed_seconds
beb_ready_recovery_elapsed_seconds
```

SQLite lifecycle fields are:

```text
database_health
sqlite_database_size_bytes
sqlite_page_count
sqlite_page_size
sqlite_freelist_count
production_records_count
daily_summary_count
oldest_raw_record_at
retention_days
cleanup_enabled
last_cleanup_at
last_cleanup_deleted_rows
last_cleanup_error
```

These SQLite lifecycle metrics are cached briefly in process to avoid running table counts and size checks on every frequent health request.

When readiness is enabled and the confirmed state is not `READY`, `/health` reports a degraded status. Readiness checks do not stop the queue worker, do not submit print/inventory business messages, and do not alter ACK or timeout handling.

Dashboard-ready read-only API routes:

```text
GET /api/v1/dashboard/production/recent
GET /api/v1/dashboard/production/daily
GET /api/v1/dashboard/production/summary
```

These routes use the same HTTP Basic API authentication as the command route, return bounded results, and expose only dashboard-safe production fields. Supported filters are `date_from`, `date_to`, `table_no`, `model`, `customer_id`, `batch_number`, `operator_id`, and `limit` where applicable. Dashboard clients should use these APIs instead of reading the SQLite file directly.

Publish command:

```bash
curl -u odoo:<password> \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: PRINTJOB-2960-T01" \
  -X POST \
  -d '{"messt01": "Z106-020C012P001"}' \
  http://127.0.0.1:8000/api/v1/plc/command
```

Authentication uses HTTP Basic credentials from `BEB_API_USERNAME` and `BEB_API_PASSWORD`. Passwords and `Authorization` headers are never logged.

Send an `Idempotency-Key` for each Odoo print-job command, for example `PRINTJOB-2960-T01`. If the same key is received again within `BEB_API_IDEMPOTENCY_TTL_SECONDS`, BEB returns the original stored response and does not publish to MQTT again. Requests without an idempotency key are accepted, but duplicate prevention is weaker.

Local smoke test:

```powershell
$env:BEB_API_URL = "http://127.0.0.1:8000"
$env:BEB_API_USERNAME = "odoo"
$env:BEB_API_PASSWORD = "<password>"
python -m scripts.test_beb_api
```

## Environment

Development defaults:

```env
MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_CLIENT_ID=BLACKBEAR_PYTHON_BRIDGE_DEV
MQTT_PLC_TO_ODOO_TOPIC=MQTT/PLC_TO_ODOO/topic
MQTT_ODOO_TO_PLC_TOPIC=MQTT/ODOO_TO_PLC/topic
MQTT_KEEPALIVE=60
DATABASE_PATH=data/bridge.db
LOG_LEVEL=INFO
ODOO_ENABLED=false
ODOO_URL=https://test-bbw.odoo.com
ODOO_DATABASE=broadtechit-test-bbw-stage-34933250
ODOO_USERNAME=admin
ODOO_PASSWORD=
ODOO_MODEL=iot.configuration
ODOO_SUBMIT_METHOD=xmlrpc_submit_print_data
ODOO_TIMEOUT=90
ODOO_WORKER_INTERVAL=2
ODOO_BATCH_SIZE=10
ODOO_MAX_RETRIES=10
ODOO_STALE_PROCESSING_SECONDS=300
ODOO_WORKER_HEARTBEAT_SECONDS=60
BEB_API_ENABLED=false
BEB_API_HOST=127.0.0.1
BEB_API_PORT=8000
BEB_API_USERNAME=odoo
BEB_API_PASSWORD=
BEB_API_REQUEST_TIMEOUT=10
BEB_API_IDEMPOTENCY_TTL_SECONDS=86400
BEB_API_MAX_BODY_BYTES=16384
BEB_API_LOG_REQUEST_BODY=true
BEB_READY_ENABLED=true
BEB_READY_CHECK_INTERVAL_SECONDS=30
BEB_READY_CHECK_TIMEOUT_SECONDS=3
BEB_READY_AUTH_REVALIDATE_SECONDS=300
BEB_READY_DISCONNECT_DELAY_SECONDS=5
BEB_READY_RECOVERY_DELAY_SECONDS=10
BEB_READY_TOPIC=MQTT/ODOO_TO_PLC/topic
BEB_MACHINE_ID=BLACKBEAR_PYTHON_BRIDGE_DEV
SQLITE_RAW_RETENTION_DAYS=30
SQLITE_CLEANUP_ENABLED=true
SQLITE_CLEANUP_INTERVAL_HOURS=24
SQLITE_CLEANUP_BATCH_SIZE=1000
SQLITE_VACUUM_ENABLED=false
SQLITE_RECONCILE_BATCH_SIZE=100
```

Production default logging is `LOG_LEVEL=INFO`. Routine successful readiness checks, cached Odoo authentication messages, zero-row stale watchdog passes, and other repetitive internal polling diagnostics are DEBUG-only. Use `LOG_LEVEL=DEBUG` during BBA testing or troubleshooting when those details are needed. Production transaction traceability remains at INFO or above for MQTT receives, Odoo API commands, command publishes, queue processing, retries, completions, readiness transitions, and BR publication.

The real `.env` file is ignored by Git. Do not print or log `ODOO_PASSWORD`.

To run without live Odoo:

```env
ODOO_ENABLED=false
```

To enable live Odoo forwarding:

```env
ODOO_ENABLED=true
```

Ubuntu example:

```env
MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_CLIENT_ID=BEB_UBUNTU_01
MQTT_PLC_TO_ODOO_TOPIC=MQTT/PLC_TO_ODOO/topic
MQTT_ODOO_TO_PLC_TOPIC=MQTT/ODOO_TO_PLC/topic
MQTT_KEEPALIVE=60

DATABASE_PATH=data/bridge.db
LOG_LEVEL=INFO

ODOO_ENABLED=true
ODOO_URL=https://test-bbw.odoo.com
ODOO_DATABASE=broadtechit-test-bbw-stage-34933250
ODOO_USERNAME=admin
ODOO_PASSWORD=
ODOO_MODEL=iot.configuration
ODOO_SUBMIT_METHOD=xmlrpc_submit_print_data
ODOO_TIMEOUT=90
ODOO_WORKER_INTERVAL=2
ODOO_BATCH_SIZE=10
ODOO_MAX_RETRIES=10
ODOO_STALE_PROCESSING_SECONDS=300
ODOO_WORKER_HEARTBEAT_SECONDS=60

BEB_API_ENABLED=true
BEB_API_HOST=127.0.0.1
BEB_API_PORT=8000
BEB_API_USERNAME=odoo
BEB_API_PASSWORD=<set-a-strong-password>
BEB_API_REQUEST_TIMEOUT=10
BEB_API_IDEMPOTENCY_TTL_SECONDS=86400
BEB_API_MAX_BODY_BYTES=16384
BEB_API_LOG_REQUEST_BODY=true
BEB_READY_ENABLED=true
BEB_READY_CHECK_INTERVAL_SECONDS=30
BEB_READY_CHECK_TIMEOUT_SECONDS=3
BEB_READY_AUTH_REVALIDATE_SECONDS=300
BEB_READY_DISCONNECT_DELAY_SECONDS=5
BEB_READY_RECOVERY_DELAY_SECONDS=10
BEB_READY_TOPIC=MQTT/ODOO_TO_PLC/topic
BEB_MACHINE_ID=BEB_BBW_TABLE1
SQLITE_RAW_RETENTION_DAYS=30
SQLITE_CLEANUP_ENABLED=true
SQLITE_CLEANUP_INTERVAL_HOURS=24
SQLITE_CLEANUP_BATCH_SIZE=1000
SQLITE_VACUUM_ENABLED=false
SQLITE_RECONCILE_BATCH_SIZE=100
```

## Windows Development

```powershell
cd F:\BlackBear\Development\BEB
.\.venv\Scripts\Activate.ps1
python -m unittest discover -s tests -v
python -m compileall app tests scripts
python -m app.main
```

Run a local MQTT broker, such as Mosquitto, before starting BEB.

Optional live Odoo check:

```powershell
python -m scripts.test_odoo_connection
```

The live check prints URL, database, username, model, method, and the Odoo response. It never prints the password.

Optional local API check:

```powershell
python -m scripts.test_beb_api
```

## Docker Deployment

Docker is the reference deployment path for BBA validation before migration to BBW production. The real `.env` file remains local to each machine and is ignored by Git. Only `.env.example` is tracked.

Deployment workflow:

```text
Developer PC
 |
 | git push
 v
BBA Ubuntu laptop
 |
 | git pull
 | docker compose up -d --build
 v
Validation
 |
 v
Deploy to BBW
```

Runtime architecture:

```text
AWS-hosted Odoo
 |
 | HTTPS / tunnel / VPN / reverse proxy
 v
BBA or BBW Docker host
 |
 +-- beb container
 |    |
 |    +-- /app/data/bridge.db
 |        persisted in Docker volume: beb-sqlite-data
 |
 +-- beb-mosquitto container
      |
      +-- /mosquitto/config/mosquitto.conf
      |   tracked from docker/mosquitto/mosquitto.conf
      |
      +-- /mosquitto/data
      |   persisted in Docker volume: beb-mosquitto-data
      |
      +-- /mosquitto/log
          persisted in Docker volume: beb-mosquitto-log
```

The production Compose stack runs two containers:

```text
beb
beb-mosquitto
```

`beb` serves the API on host `127.0.0.1:8000`, connects to Mosquitto as `mosquitto:1883` inside Docker, and stores SQLite at `/app/data/bridge.db` in the existing `beb-sqlite-data` volume. `beb-mosquitto` listens on LAN port `1883` and uses the existing Mosquitto data and log volumes. Both services use `restart: unless-stopped` and health checks.

Docker installation on Ubuntu:

```bash
sudo apt update
sudo apt install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
. /etc/os-release
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker "$USER"
newgrp docker
docker --version
docker compose version
```

First deployment on BBA:

```bash
git clone https://github.com/blackbearprojectmanagement/blackbear-edge-bridge.git
cd blackbear-edge-bridge
cp .env.example .env
nano .env
docker compose config
docker compose up -d --build
docker compose ps
```

For Docker, Compose loads deployment values from `.env` and sets container-only overrides automatically:

```text
MQTT_HOST=mosquitto
MQTT_PORT=1883
DATABASE_PATH=/app/data/bridge.db
BEB_API_ENABLED=true
BEB_API_HOST=0.0.0.0
BEB_API_PORT=8000
```

Set deployment-specific values in `.env`, especially:

```env
ODOO_ENABLED=true
ODOO_PASSWORD=<set-on-host-only>
BEB_API_ENABLED=true
BEB_API_USERNAME=odoo
BEB_API_PASSWORD=<set-on-host-only>
```

If a secret contains `$`, keep it in `.env`, single-quote the value, and do not add it directly to `docker-compose.yml`:

```env
ODOO_PASSWORD='value-with-$-characters'
BEB_API_PASSWORD='value-with-$-characters'
```

By default the API is bound to localhost on the Docker host and Mosquitto is exposed on port `1883` for PLC LAN access:

```env
BEB_API_BIND=127.0.0.1
BEB_API_HOST_PORT=8000
```

Restrict MQTT with host firewall rules and trusted LAN design. Do not expose MQTT port `1883` to the public internet.

Operational commands:

```bash
# start or update
docker compose up -d --build

# stop without deleting volumes
docker compose stop

# status
docker compose ps

# health
curl http://127.0.0.1:8000/health

# database size and lifecycle metrics
curl http://127.0.0.1:8000/health | python3 -m json.tool

# live BEB logs
docker compose logs -f beb

# live Mosquitto logs
docker compose logs -f mosquitto

# read-only BBW live monitor
./beb-monitor.sh
```

`beb-monitor.sh` is read-only. It does not restart containers, change `.env`, modify SQLite, run migrations, publish MQTT messages, or alter production behavior. It displays Docker status, `/health`, selected BEB application timestamps converted from UTC to `Asia/Kolkata`, container CPU/memory/network/block I/O, filtered Mosquitto client history for `BEB_BBW_TABLE1` and `FX5U_TEST`, SQLite integrity and status summaries, the latest `mqtt_messages`, and then live BEB logs.

Docker log rotation is configured for both containers with the `json-file` driver, `max-size: 10m`, and `max-file: 5`. This bounds Docker-managed log growth; it does not change BEB transaction logging semantics.

Updating from Git:

```bash
cd blackbear-edge-bridge
git pull
docker compose config
docker compose up -d --build
docker compose ps
```

Rebuilding without changing Git state:

```bash
docker compose build --no-cache beb
docker compose up -d
```

Restarting:

```bash
docker compose restart
docker compose restart beb
docker compose restart mosquitto
```

Viewing logs:

```bash
docker compose logs -f
docker compose logs -f beb
docker compose logs -f mosquitto
```

Health checks:

```bash
docker compose ps
curl http://127.0.0.1:8000/health
```

Backup SQLite:

```bash
mkdir -p backups
docker compose exec -T beb python -c "import sqlite3; src=sqlite3.connect('/app/data/bridge.db'); dst=sqlite3.connect('/app/data/bridge-backup.db'); src.backup(dst); src.close(); dst.close()"
docker cp beb:/app/data/bridge-backup.db "backups/bridge-$(date +%Y%m%d-%H%M%S).db"
docker compose exec -T beb rm -f /app/data/bridge-backup.db
```

Restore SQLite:

```bash
docker compose stop beb
docker cp backups/bridge-YYYYMMDD-HHMMSS.db beb:/app/data/bridge.db
docker compose start beb
docker compose logs -f beb
```

Docker troubleshooting:

```bash
docker compose config
docker compose ps
docker compose logs --tail=200 beb
docker compose logs --tail=200 mosquitto
docker inspect beb --format '{{json .State.Health}}'
docker inspect beb-mosquitto --format '{{json .State.Health}}'
docker volume ls | grep beb
docker network inspect beb-net
```

Common checks:

- If BEB cannot connect to MQTT, verify the `beb-mosquitto` container is healthy and `MQTT_HOST` resolves to `mosquitto` inside Compose.
- If `/health` is unreachable from the host, verify `BEB_API_ENABLED=true` and the `BEB_API_BIND:BEB_API_HOST_PORT` mapping.
- If Odoo forwarding is disabled, verify `ODOO_ENABLED=true` in the host `.env`.
- If Docker starts after reboot but BEB is unhealthy, review `docker compose logs beb` before rebuilding.
- If a port is already in use, change `BEB_API_HOST_PORT` or `MQTT_HOST_PORT` in `.env`.

Restart policy:

```yaml
restart: unless-stopped
```

Both BEB and Mosquitto use this policy, so containers automatically restart after an Ubuntu reboot unless they were manually stopped.

Rollback and backup warnings:

- Containers are disposable; the SQLite Docker volume is not.
- Back up `beb-sqlite-data` before production updates, rollbacks, host migration, or volume maintenance.
- Image versioning and Git rollback are separate from SQLite data backup.
- Do not delete or recreate Docker volumes during rollback unless a verified backup has been restored intentionally.
- BEB does not provide automatic rollback.

Recommended production Docker architecture:

```text
Odoo
 |
 | HTTPS through Cloudflare Tunnel, VPN, or authenticated reverse proxy
 v
BBW Docker host
 |
 +-- Reverse proxy or tunnel endpoint
 |    |
 |    v
 |   beb:8000 on private Docker network
 |
 +-- beb container
 |    |
 |    +-- SQLite Docker volume with scheduled host backups
 |    |
 |    +-- MQTT client connection to mosquitto:1883
 |
 +-- beb-mosquitto container
      |
      +-- MQTT exposed only to localhost or trusted LAN interface
      +-- Persistent Mosquitto data volume
      +-- Tracked Mosquitto config
```

For BBW production, keep secrets in the host `.env`, run `docker compose config` before each deployment, validate on BBA first, and back up `beb-sqlite-data` before migration.

## Native Ubuntu Deployment

Native Python deployment remains documented for reference. Docker is the preferred BBA validation path before BBW production migration.

```bash
sudo apt update
sudo apt install -y python3 python3-venv
git clone https://github.com/blackbearprojectmanagement/blackbear-edge-bridge.git
cd blackbear-edge-bridge
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
nano .env
python -m unittest discover -s tests -v
python -m compileall app tests scripts
python -m app.main
```

For service deployment, run BEB under `systemd` and keep Mosquitto bound locally.

```ini
[Unit]
Description=BlackBear Edge Bridge
After=network-online.target
Wants=network-online.target

[Service]
WorkingDirectory=/opt/blackbear-edge-bridge
EnvironmentFile=/opt/blackbear-edge-bridge/.env
ExecStart=/opt/blackbear-edge-bridge/.venv/bin/python -m app.main
Restart=always
RestartSec=5
User=blackbear

[Install]
WantedBy=multi-user.target
```

## Restrictions

BEB does not implement CUPS, printer communication, label design, direct printing, Odoo print logic, Odoo-to-PLC polling, an Odoo command retrieval queue, a dashboard, or frontend code.

Do not expose Mosquitto port `1883` publicly. Use Cloudflare Tunnel for pilot testing, or a site-to-site VPN/authenticated reverse proxy for permanent deployment of the HTTP API. Tunnel setup is intentionally outside BEB application code.
