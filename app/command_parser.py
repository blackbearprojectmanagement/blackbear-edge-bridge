"""Validation for Odoo-to-PLC command payloads."""

from __future__ import annotations


class CommandValidationError(ValueError):
    """Raised when an Odoo-to-PLC command payload is invalid."""


PRINT_JOB_KEYS = frozenset({"messt01", "messt02", "messt03"})
TABLE_CONTROL_KEYS = frozenset({"T01", "T02", "T03"})
TABLE_CONTROL_VALUES = frozenset({"P", "R", "D"})
LOOSE_PACKET_VALUES = frozenset({"FT01", "FT02", "FT03"})


def validate_plc_command(payload: object) -> dict[str, str]:
    """Validate and return an exact Odoo-to-PLC command payload."""
    if not isinstance(payload, dict):
        raise CommandValidationError("Payload must be a JSON object")

    if len(payload) != 1:
        raise CommandValidationError("Payload must contain exactly one top-level key")

    key, value = next(iter(payload.items()))
    if not isinstance(key, str):
        raise CommandValidationError("Payload key must be a string")
    if not isinstance(value, str):
        raise CommandValidationError("Payload value must be a string")

    if key in PRINT_JOB_KEYS:
        if value == "":
            raise CommandValidationError(f"{key} value must be non-empty")
        return {key: value}

    if key in TABLE_CONTROL_KEYS:
        if value not in TABLE_CONTROL_VALUES:
            raise CommandValidationError(f"{key} value must be P, R, or D")
        return {key: value}

    if key == "LP":
        if value not in LOOSE_PACKET_VALUES:
            raise CommandValidationError("LP value must be FT01, FT02, or FT03")
        return {key: value}

    raise CommandValidationError(f"Unsupported command key: {key}")
