# BlackBear Edge Bridge (BEB)

BlackBear Edge Bridge is a small Python service that listens for PLC MQTT messages, parses the supported PLC-to-Odoo payloads, and provides a reusable publisher for commands back to the PLC.

This milestone intentionally does not include Odoo API integration, SQLAlchemy usage, SQLite persistence, PLC topic changes, payload format changes, or a dashboard.

## Current Behavior

- Loads configuration from `.env`
- Connects to an MQTT broker with `paho-mqtt`
- Uses MQTT v3.1.1, Callback API Version 2, QoS 0
- Subscribes to `MQTT/PLC_TO_ODOO/topic`
- Parses supported UTF-8 JSON PLC messages:
  - `{"MN":"106-020C012P001 3241T01"}`
  - `{"MP":"Z106-015C020P001 7084T02"}`
- Extracts:
  - `message_type`: `MN` or `MP`
  - `part_data`: everything before the final space
  - `serial`: numeric characters after the final space and before the table suffix
  - `table`: `T01`, `T02`, or `T03`
- Rejects malformed or unsupported messages with clear log messages
- Provides `BEBMqttClient.publish_odoo_command(...)` for publishing to `MQTT/ODOO_TO_PLC/topic`
- Reconnects automatically after unexpected MQTT disconnects
- Shuts down cleanly on Ctrl+C

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
- invalid JSON
- unsupported key
- missing table suffix
- non-string value

Run tests with:

```bash
python -m unittest discover -s tests -v
```
