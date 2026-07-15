"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class AppConfig:
    mqtt_host: str
    mqtt_port: int
    mqtt_client_id: str
    mqtt_plc_to_odoo_topic: str
    mqtt_odoo_to_plc_topic: str
    mqtt_keepalive: int
    database_path: Path
    log_level: str


def _get_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default

    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def load_config() -> AppConfig:
    """Load application configuration from .env and process environment."""
    load_dotenv()

    return AppConfig(
        mqtt_host=os.getenv("MQTT_HOST", "localhost"),
        mqtt_port=_get_int("MQTT_PORT", 1883),
        mqtt_client_id=os.getenv("MQTT_CLIENT_ID", "BLACKBEAR_PYTHON_BRIDGE_DEV"),
        mqtt_plc_to_odoo_topic=os.getenv(
            "MQTT_PLC_TO_ODOO_TOPIC", "MQTT/PLC_TO_ODOO/topic"
        ),
        mqtt_odoo_to_plc_topic=os.getenv(
            "MQTT_ODOO_TO_PLC_TOPIC", "MQTT/ODOO_TO_PLC/topic"
        ),
        mqtt_keepalive=_get_int("MQTT_KEEPALIVE", 60),
        database_path=Path(os.getenv("DATABASE_PATH", "data/bridge.db")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )
