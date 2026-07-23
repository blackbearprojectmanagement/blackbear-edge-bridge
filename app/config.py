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
    odoo_enabled: bool
    odoo_url: str
    odoo_database: str
    odoo_username: str
    odoo_password: str
    odoo_model: str
    odoo_submit_method: str
    odoo_timeout: int
    odoo_worker_interval: int
    odoo_batch_size: int
    odoo_max_retries: int
    odoo_stale_processing_seconds: int
    odoo_worker_heartbeat_seconds: int = 60
    beb_api_enabled: bool = False
    beb_api_host: str = "127.0.0.1"
    beb_api_port: int = 8000
    beb_api_username: str = "odoo"
    beb_api_password: str = ""
    beb_api_request_timeout: int = 10
    beb_api_idempotency_ttl_seconds: int = 86400
    beb_api_max_body_bytes: int = 16384
    beb_api_log_request_body: bool = True
    beb_ready_enabled: bool = True
    beb_ready_check_interval_seconds: int = 30
    beb_ready_check_timeout_seconds: int = 3
    beb_ready_auth_revalidate_seconds: int = 300
    beb_ready_disconnect_delay_seconds: int = 5
    beb_ready_recovery_delay_seconds: int = 10
    beb_ready_topic: str = "MQTT/ODOO_TO_PLC/topic"
    machine_id: str = "BEB"
    sqlite_raw_retention_days: int = 30
    sqlite_cleanup_enabled: bool = True
    sqlite_cleanup_interval_hours: int = 24
    sqlite_cleanup_batch_size: int = 1000
    sqlite_vacuum_enabled: bool = False
    sqlite_reconcile_batch_size: int = 100


def _get_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default

    try:
        return int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _get_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None or raw_value.strip() == "":
        return default

    normalized = raw_value.strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False

    raise ValueError(f"{name} must be a boolean value")


def load_config() -> AppConfig:
    """Load application configuration from .env and process environment."""
    load_dotenv()
    mqtt_client_id = os.getenv("MQTT_CLIENT_ID", "BLACKBEAR_PYTHON_BRIDGE_DEV")

    return AppConfig(
        mqtt_host=os.getenv("MQTT_HOST", "localhost"),
        mqtt_port=_get_int("MQTT_PORT", 1883),
        mqtt_client_id=mqtt_client_id,
        mqtt_plc_to_odoo_topic=os.getenv(
            "MQTT_PLC_TO_ODOO_TOPIC", "MQTT/PLC_TO_ODOO/topic"
        ),
        mqtt_odoo_to_plc_topic=os.getenv(
            "MQTT_ODOO_TO_PLC_TOPIC", "MQTT/ODOO_TO_PLC/topic"
        ),
        mqtt_keepalive=_get_int("MQTT_KEEPALIVE", 60),
        database_path=Path(os.getenv("DATABASE_PATH", "data/bridge.db")),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        machine_id=os.getenv("BEB_MACHINE_ID", mqtt_client_id),
        odoo_enabled=_get_bool("ODOO_ENABLED", False),
        odoo_url=os.getenv("ODOO_URL", "https://test-bbw.odoo.com"),
        odoo_database=os.getenv(
            "ODOO_DATABASE", "broadtechit-test-bbw-stage-34933250"
        ),
        odoo_username=os.getenv("ODOO_USERNAME", "admin"),
        odoo_password=os.getenv("ODOO_PASSWORD", ""),
        odoo_model=os.getenv("ODOO_MODEL", "iot.configuration"),
        odoo_submit_method=os.getenv(
            "ODOO_SUBMIT_METHOD", "xmlrpc_submit_print_data"
        ),
        odoo_timeout=_get_int("ODOO_TIMEOUT", 90),
        odoo_worker_interval=_get_int("ODOO_WORKER_INTERVAL", 2),
        odoo_batch_size=_get_int("ODOO_BATCH_SIZE", 10),
        odoo_max_retries=_get_int("ODOO_MAX_RETRIES", 10),
        odoo_stale_processing_seconds=_get_int(
            "ODOO_STALE_PROCESSING_SECONDS", 300
        ),
        odoo_worker_heartbeat_seconds=_get_int(
            "ODOO_WORKER_HEARTBEAT_SECONDS", 60
        ),
        beb_api_enabled=_get_bool("BEB_API_ENABLED", False),
        beb_api_host=os.getenv("BEB_API_HOST", "127.0.0.1"),
        beb_api_port=_get_int("BEB_API_PORT", 8000),
        beb_api_username=os.getenv("BEB_API_USERNAME", "odoo"),
        beb_api_password=os.getenv("BEB_API_PASSWORD", ""),
        beb_api_request_timeout=_get_int("BEB_API_REQUEST_TIMEOUT", 10),
        beb_api_idempotency_ttl_seconds=_get_int(
            "BEB_API_IDEMPOTENCY_TTL_SECONDS", 86400
        ),
        beb_api_max_body_bytes=_get_int("BEB_API_MAX_BODY_BYTES", 16384),
        beb_api_log_request_body=_get_bool("BEB_API_LOG_REQUEST_BODY", True),
        beb_ready_enabled=_get_bool("BEB_READY_ENABLED", True),
        beb_ready_check_interval_seconds=_get_int(
            "BEB_READY_CHECK_INTERVAL_SECONDS", 30
        ),
        beb_ready_check_timeout_seconds=_get_int(
            "BEB_READY_CHECK_TIMEOUT_SECONDS", 3
        ),
        beb_ready_auth_revalidate_seconds=_get_int(
            "BEB_READY_AUTH_REVALIDATE_SECONDS", 300
        ),
        beb_ready_disconnect_delay_seconds=_get_int(
            "BEB_READY_DISCONNECT_DELAY_SECONDS", 5
        ),
        beb_ready_recovery_delay_seconds=_get_int(
            "BEB_READY_RECOVERY_DELAY_SECONDS", 10
        ),
        beb_ready_topic=os.getenv(
            "BEB_READY_TOPIC", "MQTT/ODOO_TO_PLC/topic"
        ),
        sqlite_raw_retention_days=_get_int("SQLITE_RAW_RETENTION_DAYS", 30),
        sqlite_cleanup_enabled=_get_bool("SQLITE_CLEANUP_ENABLED", True),
        sqlite_cleanup_interval_hours=_get_int("SQLITE_CLEANUP_INTERVAL_HOURS", 24),
        sqlite_cleanup_batch_size=_get_int("SQLITE_CLEANUP_BATCH_SIZE", 1000),
        sqlite_vacuum_enabled=_get_bool("SQLITE_VACUUM_ENABLED", False),
        sqlite_reconcile_batch_size=_get_int("SQLITE_RECONCILE_BATCH_SIZE", 100),
    )
