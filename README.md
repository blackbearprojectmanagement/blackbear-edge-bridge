# BlackBear Edge Bridge (BEB)

BlackBear Edge Bridge is a Python service that listens for PLC MQTT messages, validates and stores them in a local SQLite queue, then optionally forwards queued messages to Odoo through XML-RPC.

BEB does not print. Odoo continues to control the existing CUPS printing workflow, label templates, print routing, and printer communication.

## Architecture

```text
Mitsubishi FX5U PLC
 |
 v
Local MQTT / Mosquitto Broker
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

Milestone 4 does not send Odoo-to-PLC MQTT commands.

Supported PLC payloads:

```json
{"MN":"106-020C012P001 3242T01"}
{"MP":"Z106-015C020P001 7084T01"}
```

`MN` means Print Completed. `MP` means Loose Packet.

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
- `PROCESSING` rows older than `ODOO_STALE_PROCESSING_SECONDS` are recovered as `FAILED` on worker startup.
- Recovered stale rows store `Recovered stale PROCESSING message after application restart` in `last_error`.

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

Any successful XML-RPC return marks the row `COMPLETED`. Faults, protocol errors, timeouts, and connection errors mark the row `FAILED` and allow later retry.

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
ODOO_PASSWORD=TEst@#$mvjurT
ODOO_MODEL=iot.configuration
ODOO_SUBMIT_METHOD=xmlrpc_submit_print_data
ODOO_TIMEOUT=15
ODOO_WORKER_INTERVAL=2
ODOO_BATCH_SIZE=10
ODOO_MAX_RETRIES=10
ODOO_STALE_PROCESSING_SECONDS=300
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
ODOO_PASSWORD=TEst@#$mvjurT
ODOO_MODEL=iot.configuration
ODOO_SUBMIT_METHOD=xmlrpc_submit_print_data
ODOO_TIMEOUT=15
ODOO_WORKER_INTERVAL=2
ODOO_BATCH_SIZE=10
ODOO_MAX_RETRIES=10
ODOO_STALE_PROCESSING_SECONDS=300
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

BEB does not implement CUPS, printer communication, label design, direct printing, Odoo print logic, Odoo-to-PLC polling, pause/resume/done workflows, a dashboard, or a web framework.
