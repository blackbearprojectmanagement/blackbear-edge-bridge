"""Parse PLC-to-Odoo MQTT messages."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


SUPPORTED_MESSAGE_TYPES = frozenset({"MN", "MP"})
SUPPORTED_TABLES = frozenset({"T01", "T02", "T03"})
COMPACT_VALUE_PATTERN = re.compile(r"^(?P<model>.+P\d{3})(?P<serial>\d+)$")


class PLCMessageParseError(ValueError):
    """Raised when a PLC message cannot be parsed according to the contract."""


@dataclass(frozen=True, slots=True)
class ParsedPLCMessage:
    message_type: str
    model_number: str
    serial_number: str
    table_number: str

    @property
    def part_data(self) -> str:
        """Backward-compatible alias for the parsed model number."""
        return self.model_number

    @property
    def serial(self) -> str:
        """Backward-compatible alias for the parsed serial number."""
        return self.serial_number

    @property
    def table(self) -> str:
        """Backward-compatible alias for the parsed table number."""
        return self.table_number


def parse_plc_message(payload: str | bytes) -> ParsedPLCMessage:
    """Parse a PLC JSON payload into a typed message object."""
    text = _decode_payload(payload)
    data = _load_json_object(text)

    if len(data) != 1:
        raise PLCMessageParseError("PLC message must contain exactly one field")

    message_type, value = next(iter(data.items()))
    if message_type not in SUPPORTED_MESSAGE_TYPES:
        raise PLCMessageParseError(f"Unsupported PLC message type: {message_type}")

    if not isinstance(value, str):
        raise PLCMessageParseError(f"PLC message value for {message_type} must be a string")

    model_number, serial_number, table_number = _split_message_value(value)

    return ParsedPLCMessage(
        message_type=message_type,
        model_number=model_number,
        serial_number=serial_number,
        table_number=table_number,
    )


def _decode_payload(payload: str | bytes) -> str:
    if isinstance(payload, bytes):
        try:
            return payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PLCMessageParseError("PLC payload must be valid UTF-8") from exc

    return payload


def _load_json_object(text: str) -> dict[str, Any]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PLCMessageParseError(f"Malformed JSON: {exc.msg}") from exc

    if not isinstance(data, dict):
        raise PLCMessageParseError("PLC message must be a JSON object")

    return data


def _split_message_value(value: str) -> tuple[str, str, str]:
    value = value.strip()
    table_number = value[-3:]
    payload_without_table = value[:-3]

    if table_number not in SUPPORTED_TABLES:
        raise PLCMessageParseError("PLC message is missing a supported table suffix")

    if " " in payload_without_table:
        model_number, serial_number = _split_spaced_payload(payload_without_table)
    else:
        model_number, serial_number = _split_compact_payload(payload_without_table)

    if not model_number:
        raise PLCMessageParseError("PLC model number is missing")

    if not serial_number or not serial_number.isdigit():
        raise PLCMessageParseError("PLC serial must contain only numeric characters")

    return model_number, serial_number, table_number


def _split_spaced_payload(payload_without_table: str) -> tuple[str, str]:
    try:
        model_number, serial_number = payload_without_table.rsplit(" ", maxsplit=1)
    except ValueError as exc:
        raise PLCMessageParseError(
            "PLC message value must contain model and serial data"
        ) from exc

    return model_number.strip(), serial_number.strip()


def _split_compact_payload(payload_without_table: str) -> tuple[str, str]:
    match = COMPACT_VALUE_PATTERN.fullmatch(payload_without_table)
    if match is None:
        raise PLCMessageParseError(
            "Compact PLC payload must contain a model ending like P001 followed by a numeric serial"
        )

    return match.group("model"), match.group("serial")
