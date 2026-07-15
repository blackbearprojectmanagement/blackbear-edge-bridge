# BlackBear Edge Bridge (BEB)

BlackBear Edge Bridge is a small Python service that listens for PLC MQTT messages, parses supported PLC-to-Odoo payloads, and stores valid messages in a local SQLite queue before any future processing.

This milestone intentionally does not include Odoo API integration, SQLAlchemy usage, printing, PLC topic changes, payload format changes, or a dashboard.

## Architecture

```text
PLC
 |
 v
MQTT
 |
 v
Mosquitto
 |
 v
BEB
 |
 v
SQLite Queue
 |
 v
Future Odoo API
```

## Current Behavior

- Loads configuration from `.env`
- Connects to an MQTT broker with `paho-mqtt`
- Uses MQTT v3.1.1, Callback API Version 2, QoS 0
- Subscribes to `MQTT/PLC_TO_ODOO/topic`
- Parses supported UTF-8 JSON PLC messages:
  - `{"MN":"106-020C012P0013241T01"}`
  - `{"MP":"Z106-015C020P0017084T02"}`
- Extracts:
  - `message_type`: `MN` or `MP`
  - `model_number`: model data before the serial number
  - `serial_number`: numeric serial characters before the table suffix
  - `table_number`: `T01`, `T02`, or `T03`
- Logs each accepted MQTT message with timestamp, topic, raw payload, message type, table, model, and serial
- Stores each valid message in `data/bridge.db` before any future processing
- Uses a SHA256 hash of `topic + raw_payload` as the unique message identity
- Ignores duplicate MQTT deliveries and logs `Duplicate message ignored`
- Rejects malformed or unsupported messages with clear log messages
- Provides `BEBMqttClient.publish_odoo_command(...)` for publishing to `MQTT/ODOO_TO_PLC/topic`
- Reconnects automatically after unexpected MQTT disconnects
- Shuts down cleanly on Ctrl+C

`MN` means Print Completed. `MP` means Loose Packet.

The current compact PLC payload format puts the table number in the final three characters:

```text
106-020C012P0013241T01
```

This is parsed as:

```text
Model  : 106-020C012P001
Serial : 3241
Table  : T01
```

For compatibility with the first milestone, the parser also accepts the earlier spaced format:

```text
106-020C012P001 3241T01
```

Example receive log:

```text
--------------------------------------------------
Received MQTT Message
Timestamp  : 2026-07-15T15:40:12+05:30
Topic      : MQTT/PLC_TO_ODOO/topic
Raw Payload: {"MN":"106-020C012P0013241T01"}
Type       : MN (Print Completed)
Table      : T01
Model      : 106-020C012P001
Serial     : 3241
Saved to SQLite
ID         : 18
Hash       : 53f4d8...
Status     : NEW
--------------------------------------------------
```

## SQLite Queue

The database is created automatically at `data/bridge.db` on startup or on the first message save.

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
```

Supported statuses:

```text
NEW
PROCESSING
COMPLETED
FAILED
```

Current receive flow:

```text
Receive MQTT
 |
 v
Parse JSON
 |
 v
Generate SHA256 hash from topic + raw payload
 |
 v
Ignore duplicate or insert NEW row into SQLite
 |
 v
Log saved message details
```

No Odoo calls are made yet.

## Configuration

Copy `.env.example` to `.env` for local development and adjust values as needed.

```env
MQTT_HOST=localhost
MQTT_PORT=1883
MQTT_CLIENT_ID=BLACKBEAR_PYTHON_BRIDGE_DEV
MQTT_PLC_TO_ODOO_TOPIC=MQTT/PLC_TO_ODOO/topic
MQTT_ODOO_TO_PLC_TOPIC=MQTT/ODOO_TO_PLC/topic
MQTT_KEEPALIVE=60
DATABASE_PATH=data/bridge.db
LOG_LEVEL=INFO
```

The real `.env` file is ignored by Git and must not be committed.

## Windows Development

From PowerShell:

```powershell
cd F:\BlackBear\Development\BEB
.\.venv\Scripts\Activate.ps1
python -m unittest discover -s tests -v
python -m compileall app tests
python -m app.main
```

If PowerShell blocks virtual environment activation, allow scripts for the current user:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Run an MQTT broker locally, such as Mosquitto, before starting the bridge. The default configuration expects the broker at `localhost:1883`.

## Ubuntu Deployment

Install Python, create a virtual environment, install dependencies, and provide a production `.env`:

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
python -m compileall app tests
python -m app.main
```

For a long-running deployment, run the bridge under a process manager such as `systemd`.

Example service file:

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

Adjust paths, user, broker host, and permissions for the target server.

## Parser Tests

The parser unit tests cover:

- valid `MN`
- valid `MP`
- compatibility with the earlier spaced payload format
- invalid JSON
- unsupported key
- missing table suffix
- non-string value
- database creation
- message insert
- duplicate detection
- status update

Run tests with:

```bash
python -m unittest discover -s tests -v
```
