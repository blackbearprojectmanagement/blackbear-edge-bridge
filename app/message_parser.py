"""Parse PLC-to-Odoo MQTT messages."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


SUPPORTED_MESSAGE_TYPES = frozenset({"MN", "MP"})
SUPPORTED_TABLES = frozenset({"T01", "T02", "T03"})


class PLCMessageParseError(ValueError):
    """Raised when a PLC message cannot be parsed according to the contract."""


@dataclass(frozen=True, slots=True)
class ParsedPLCMessage:
    message_type: str
    part_data: str
    serial: str
    table: str


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

    part_data, serial_with_table = _split_message_value(value)
    serial, table = _split_serial_and_table(serial_with_table)

    return ParsedPLCMessage(
        message_type=message_type,
        part_data=part_data,
        serial=serial,
        table=table,
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


def _split_message_value(value: str) -> tuple[str, str]:
    try:
        part_data, serial_with_table = value.rsplit(" ", maxsplit=1)
    except ValueError as exc:
        raise PLCMessageParseError(
            "PLC message value must contain part data and serial/table separated by a space"
        ) from exc

    if not part_data:
        raise PLCMessageParseError("PLC part data is missing")

    return part_data, serial_with_table


def _split_serial_and_table(serial_with_table: str) -> tuple[str, str]:
    table = serial_with_table[-3:]
    serial = serial_with_table[:-3]

    if table not in SUPPORTED_TABLES:
        raise PLCMessageParseError("PLC message is missing a supported table suffix")

    if not serial or not serial.isdigit():
        raise PLCMessageParseError("PLC serial must contain only numeric characters")

    return serial, table
