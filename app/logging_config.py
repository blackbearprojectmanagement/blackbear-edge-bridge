"""Logging configuration helpers for BEB application logs."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


try:
    IST = ZoneInfo("Asia/Kolkata")
except ZoneInfoNotFoundError:
    # Some Windows hosts do not ship the IANA timezone database; Kolkata has no DST.
    IST = timezone(timedelta(hours=5, minutes=30), "Asia/Kolkata")
LOG_FORMAT = "[%(asctime)s] [%(beb_component)s] %(message)s"
LOGGER_COMPONENTS = {
    "app.main": "MAIN",
    "app.api": "API",
    "app.api_server": "API_SERVER",
    "app.mqtt_client": "MQTT",
    "app.queue_worker": "QUEUE_WORKER",
    "app.readiness": "READINESS",
    "app.database": "DATABASE",
    "app.sqlite_lifecycle": "SQLITE_LIFECYCLE",
    "app.odoo_client": "ODOO",
}


class ISTFormatter(logging.Formatter):
    """Format log record timestamps in Indian Standard Time for display only."""

    def __init__(self) -> None:
        super().__init__(LOG_FORMAT)

    def format(self, record: logging.LogRecord) -> str:
        record.beb_component = LOGGER_COMPONENTS.get(
            record.name,
            _component_from_logger_name(record.name),
        )
        return super().format(record)

    def formatTime(
        self,
        record: logging.LogRecord,
        datefmt: str | None = None,
    ) -> str:
        del datefmt
        value = datetime.fromtimestamp(record.created, IST)
        return (
            value.strftime("%d-%b-%Y %I:%M:%S.")
            + f"{int(record.msecs):03d} "
            + value.strftime("%p IST")
        )


def configure_ist_logging(level_name: str) -> None:
    """Configure root console/file handlers to display BEB logs in IST."""
    level = getattr(logging, level_name, logging.INFO)
    formatter = ISTFormatter()
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if not root_logger.handlers:
        root_logger.addHandler(logging.StreamHandler())

    for handler in root_logger.handlers:
        handler.setFormatter(formatter)


def _component_from_logger_name(logger_name: str) -> str:
    name = logger_name
    if name.startswith("app."):
        name = name.removeprefix("app.")
    return name.replace(".", "_").upper()
