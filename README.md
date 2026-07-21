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

At startup the readiness state is unknown/not ready. BEB publishes `BR=1` only after 10 seconds of continuous successful Odoo checks. If the first check fails, BEB publishes `BR=0` once. From READY, failures must continue for 5 seconds before BEB publishes `BR=0`; from NOT_READY, successes must continue for 10 seconds before BEB publishes `BR=1`. Brief opposite results reset the active debounce timer. Each readiness check uses the short `BEB_READY_CHECK_TIMEOUT_SECONDS` timeout, default `3` seconds, for both `common.version()` and authentication. It does not inherit the production print/inventory `ODOO_TIMEOUT=90`.

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

The health response preserves the existing fields and also reports worker and readiness state. Readiness fields are:

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

When readiness is enabled and the confirmed state is not `READY`, `/health` reports a degraded status. Readiness checks do not stop the queue worker, do not submit print/inventory business messages, and do not alter ACK or timeout handling.

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
BEB_READY_CHECK_INTERVAL_SECONDS=1
BEB_READY_CHECK_TIMEOUT_SECONDS=3
BEB_READY_DISCONNECT_DELAY_SECONDS=5
BEB_READY_RECOVERY_DELAY_SECONDS=10
BEB_READY_TOPIC=MQTT/ODOO_TO_PLC/topic
```

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
BEB_READY_CHECK_INTERVAL_SECONDS=1
BEB_READY_CHECK_TIMEOUT_SECONDS=3
BEB_READY_DISCONNECT_DELAY_SECONDS=5
BEB_READY_RECOVERY_DELAY_SECONDS=10
BEB_READY_TOPIC=MQTT/ODOO_TO_PLC/topic
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

## Ubuntu Deployment

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
