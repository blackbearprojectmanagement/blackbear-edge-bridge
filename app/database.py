"""SQLite persistence and queue state for received MQTT messages."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal


LOGGER = logging.getLogger(__name__)
DEFAULT_DATABASE_PATH = Path("data/bridge.db")
VALID_STATUSES = frozenset({"NEW", "PROCESSING", "COMPLETED", "FAILED"})
StatusValue = Literal["NEW", "PROCESSING", "COMPLETED", "FAILED"]
API_COMMAND_STATUSES = frozenset(
    {"RECEIVED", "PUBLISHED", "FAILED", "DUPLICATE", "REJECTED"}
)
ApiCommandStatusValue = Literal[
    "RECEIVED", "PUBLISHED", "FAILED", "DUPLICATE", "REJECTED"
]

BASE_COLUMNS = {
    "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
    "received_at": "TEXT NOT NULL",
    "topic": "TEXT NOT NULL",
    "message_type": "TEXT NOT NULL",
    "table_no": "TEXT NOT NULL",
    "model": "TEXT NOT NULL",
    "serial": "TEXT NOT NULL",
    "raw_payload": "TEXT NOT NULL",
    "message_hash": "TEXT NOT NULL UNIQUE",
    "status": "TEXT NOT NULL",
    "retry_count": "INTEGER DEFAULT 0",
    "processed_at": "TEXT",
}
MIGRATION_COLUMNS = {
    "last_error": "TEXT",
    "odoo_response": "TEXT",
    "last_attempt_at": "TEXT",
    "completed_at": "TEXT",
    "ack": "TEXT",
    "customer_id": "INTEGER",
    "customer_name": "TEXT",
    "operator_id": "INTEGER",
    "operator_name": "TEXT",
    "batch_number": "TEXT",
    "ack_replay_count": "INTEGER DEFAULT 0",
    "last_ack_replayed_at": "TEXT",
}
ACK_REPLAY_MAX_COUNT = 3
ACK_REPLAY_MIN_INTERVAL = timedelta(seconds=2)
ACK_REPLAY_WINDOW = timedelta(seconds=30)
DEFAULT_MACHINE_ID = "BEB"
SUMMARY_TEXT_SENTINEL = ""
SUMMARY_INT_SENTINEL = -1


@dataclass(frozen=True, slots=True)
class SavedMessage:
    id: int
    message_hash: str
    status: str
    inserted: bool


@dataclass(frozen=True, slots=True)
class MessageRecord:
    id: int
    received_at: str
    topic: str
    message_type: str
    table_no: str
    model: str
    serial: str
    raw_payload: str
    message_hash: str
    status: str
    retry_count: int
    processed_at: str | None
    last_error: str | None = None
    odoo_response: str | None = None
    last_attempt_at: str | None = None
    completed_at: str | None = None
    ack: str | None = None
    customer_id: int | None = None
    customer_name: str | None = None
    operator_id: int | None = None
    operator_name: str | None = None
    batch_number: str | None = None
    ack_replay_count: int = 0
    last_ack_replayed_at: str | None = None


MqttMessageRecord = MessageRecord


@dataclass(frozen=True, slots=True)
class AckReplayAttempt:
    allowed: bool
    reason: str
    replay_count: int
    last_ack_replayed_at: str | None


@dataclass(frozen=True, slots=True)
class ApiCommandRecord:
    id: int
    request_id: str
    idempotency_key: str | None
    received_at: str
    username: str | None
    remote_address: str | None
    payload: str
    payload_hash: str
    mqtt_topic: str
    status: str
    mqtt_rc: int | None
    mqtt_mid: int | None
    published_at: str | None
    response_code: int | None
    response_body: str | None
    last_error: str | None


@dataclass(frozen=True, slots=True)
class ProductionRecord:
    id: int
    mqtt_message_id: int
    completed_at: str
    production_date: str
    message_type: str
    machine_id: str
    table_no: str
    model: str
    serial: str
    ack: str
    customer_id: int | None
    customer_name: str | None
    operator_id: int | None
    operator_name: str | None
    number_of_operators: int | None
    batch_number: str | None
    status: str
    raw_odoo_response: str
    created_at: str
    summary_applied: bool


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    scanned_rows: int
    recovered_records: int
    recovered_summaries: int
    skipped_rows: int


@dataclass(frozen=True, slots=True)
class CleanupResult:
    deleted_mqtt_messages: int
    deleted_api_commands: int
    deleted_production_records: int
    vacuum_ran: bool = False
    error: str | None = None

    @property
    def total_deleted_rows(self) -> int:
        return (
            self.deleted_mqtt_messages
            + self.deleted_api_commands
            + self.deleted_production_records
        )


def initialize_database(database_path: str | Path = DEFAULT_DATABASE_PATH) -> None:
    """Create or migrate the SQLite database without destroying existing data."""
    db_path = Path(database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with _open_connection(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS mqtt_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT NOT NULL,
                topic TEXT NOT NULL,
                message_type TEXT NOT NULL,
                table_no TEXT NOT NULL,
                model TEXT NOT NULL,
                serial TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                message_hash TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                processed_at TEXT
            )
            """
        )
        _migrate_mqtt_messages(connection)
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mqtt_messages_status_id
            ON mqtt_messages (status, id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mqtt_messages_status_retry_id
            ON mqtt_messages (status, retry_count, id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mqtt_messages_status_completed_at
            ON mqtt_messages (status, completed_at)
            """
        )
        _initialize_production_tables(connection)


def initialize_api_commands_table(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> None:
    """Create the API command audit table without changing queue data."""
    db_path = Path(database_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with _open_connection(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS api_commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL UNIQUE,
                idempotency_key TEXT UNIQUE,
                received_at TEXT NOT NULL,
                username TEXT,
                remote_address TEXT,
                payload TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                mqtt_topic TEXT NOT NULL,
                status TEXT NOT NULL,
                mqtt_rc INTEGER,
                mqtt_mid INTEGER,
                published_at TEXT,
                response_code INTEGER,
                response_body TEXT,
                last_error TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_api_commands_idempotency_key
            ON api_commands (idempotency_key)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_api_commands_request_id
            ON api_commands (request_id)
            """
        )


def create_api_command_record(
    *,
    request_id: str,
    idempotency_key: str | None,
    username: str | None,
    remote_address: str | None,
    payload: str,
    mqtt_topic: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    status: ApiCommandStatusValue = "RECEIVED",
    received_at: datetime | None = None,
) -> ApiCommandRecord:
    """Create an API command audit row."""
    if status not in API_COMMAND_STATUSES:
        raise ValueError(f"Unsupported API command status: {status}")

    initialize_api_commands_table(database_path)
    received_at_text = _format_timestamp(received_at or datetime.now(timezone.utc))
    payload_hash = generate_api_payload_hash(payload)

    with _open_connection(database_path) as connection:
        cursor = connection.execute(
            """
            INSERT INTO api_commands (
                request_id,
                idempotency_key,
                received_at,
                username,
                remote_address,
                payload,
                payload_hash,
                mqtt_topic,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                idempotency_key,
                received_at_text,
                username,
                remote_address,
                payload,
                payload_hash,
                mqtt_topic,
                status,
            ),
        )
        row = connection.execute(
            """
            SELECT *
            FROM api_commands
            WHERE id = ?
            """,
            (int(cursor.lastrowid),),
        ).fetchone()

    return _api_command_from_row(row)


def get_api_command_by_idempotency_key(
    idempotency_key: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> ApiCommandRecord | None:
    """Return an API command audit row by Idempotency-Key."""
    initialize_api_commands_table(database_path)

    with _open_connection(database_path) as connection:
        row = connection.execute(
            """
            SELECT *
            FROM api_commands
            WHERE idempotency_key = ?
            LIMIT 1
            """,
            (idempotency_key,),
        ).fetchone()

    return _api_command_from_row(row) if row is not None else None


def mark_api_command_published(
    request_id: str,
    mqtt_rc: int,
    mqtt_mid: int | None,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    published_at: datetime | None = None,
) -> None:
    """Mark an API command as published to MQTT."""
    _update_api_command_result(
        request_id=request_id,
        status="PUBLISHED",
        mqtt_rc=mqtt_rc,
        mqtt_mid=mqtt_mid,
        published_at=_format_timestamp(published_at or datetime.now(timezone.utc)),
        last_error=None,
        database_path=database_path,
    )


def mark_api_command_failed(
    request_id: str,
    error: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    mqtt_rc: int | None = None,
    mqtt_mid: int | None = None,
) -> None:
    """Mark an API command as failed."""
    _update_api_command_result(
        request_id=request_id,
        status="FAILED",
        mqtt_rc=mqtt_rc,
        mqtt_mid=mqtt_mid,
        published_at=None,
        last_error=error,
        database_path=database_path,
    )


def mark_api_command_rejected(
    request_id: str,
    error: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> None:
    """Mark an API command as rejected."""
    _update_api_command_result(
        request_id=request_id,
        status="REJECTED",
        mqtt_rc=None,
        mqtt_mid=None,
        published_at=None,
        last_error=error,
        database_path=database_path,
    )


def save_api_response(
    request_id: str,
    response_code: int,
    response_body: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> None:
    """Persist the response sent for an API command."""
    initialize_api_commands_table(database_path)

    with _open_connection(database_path) as connection:
        cursor = connection.execute(
            """
            UPDATE api_commands
            SET response_code = ?,
                response_body = ?
            WHERE request_id = ?
            """,
            (response_code, response_body, request_id),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"API command request_id not found: {request_id}")


def save_message(
    *,
    topic: str,
    raw_payload: str,
    message_type: str,
    table_no: str,
    model: str,
    serial: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    received_at: datetime | None = None,
) -> SavedMessage:
    """Persist a parsed MQTT message unless its topic/payload hash already exists."""
    initialize_database(database_path)
    received_at_text = _format_timestamp(received_at or datetime.now(timezone.utc))
    message_hash = generate_message_hash(topic, raw_payload)

    with _open_connection(database_path) as connection:
        existing = _get_saved_message_by_hash(connection, message_hash)
        if existing is not None:
            return existing

        try:
            cursor = connection.execute(
                """
                INSERT INTO mqtt_messages (
                    received_at,
                    topic,
                    message_type,
                    table_no,
                    model,
                    serial,
                    raw_payload,
                    message_hash,
                    status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    received_at_text,
                    topic,
                    message_type,
                    table_no,
                    model,
                    serial,
                    raw_payload,
                    message_hash,
                    "NEW",
                ),
            )
        except sqlite3.IntegrityError:
            duplicate = _get_saved_message_by_hash(connection, message_hash)
            if duplicate is not None:
                return duplicate
            raise

        return SavedMessage(
            id=int(cursor.lastrowid),
            message_hash=message_hash,
            status="NEW",
            inserted=True,
        )


def message_exists(
    message_hash: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> bool:
    """Return whether a message hash already exists in the queue."""
    initialize_database(database_path)

    with _open_connection(database_path) as connection:
        row = connection.execute(
            "SELECT 1 FROM mqtt_messages WHERE message_hash = ? LIMIT 1",
            (message_hash,),
        ).fetchone()

    return row is not None


def get_pending_messages(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    limit: int = 100,
) -> list[MessageRecord]:
    """Return NEW messages in insertion order for legacy callers."""
    initialize_database(database_path)

    with _open_connection(database_path) as connection:
        rows = connection.execute(
            f"""
            SELECT {_record_columns_sql()}
            FROM mqtt_messages
            WHERE status = ?
            ORDER BY id ASC
            LIMIT ?
            """,
            ("NEW", limit),
        ).fetchall()

    return [_record_from_row(row) for row in rows]


def update_status(
    message_id: int,
    status: StatusValue,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    processed_at: datetime | None = None,
) -> None:
    """Update status for legacy callers while preserving the queue schema."""
    if status not in VALID_STATUSES:
        raise ValueError(f"Unsupported message status: {status}")

    initialize_database(database_path)
    processed_at_text = _format_timestamp(processed_at) if processed_at else None
    if status in {"COMPLETED", "FAILED"} and processed_at_text is None:
        processed_at_text = _format_timestamp(datetime.now(timezone.utc))

    with _open_connection(database_path) as connection:
        cursor = connection.execute(
            """
            UPDATE mqtt_messages
            SET status = ?, processed_at = ?
            WHERE id = ?
            """,
            (status, processed_at_text, message_id),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"Message id not found: {message_id}")


def claim_pending_messages(
    batch_size: int,
    max_retries: int,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> list[MessageRecord]:
    """Atomically claim NEW and retryable FAILED messages for Odoo processing."""
    initialize_database(database_path)
    if batch_size <= 0:
        return []

    last_attempt_at = _format_timestamp(datetime.now(timezone.utc))
    connection = _connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        rows = connection.execute(
            """
            SELECT id
            FROM mqtt_messages
            WHERE status = ?
               OR (status = ? AND retry_count < ?)
            ORDER BY id ASC
            LIMIT ?
            """,
            ("NEW", "FAILED", max_retries, batch_size),
        ).fetchall()
        message_ids = [int(row["id"]) for row in rows]
        if not message_ids:
            connection.commit()
            return []

        placeholders = ",".join("?" for _ in message_ids)
        connection.execute(
            f"""
            UPDATE mqtt_messages
            SET status = ?, last_attempt_at = ?
            WHERE id IN ({placeholders})
            """,
            ("PROCESSING", last_attempt_at, *message_ids),
        )
        claimed_rows = connection.execute(
            f"""
            SELECT {_record_columns_sql()}
            FROM mqtt_messages
            WHERE id IN ({placeholders})
            ORDER BY id ASC
            """,
            message_ids,
        ).fetchall()
        connection.commit()
        return [_record_from_row(row) for row in claimed_rows]
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def mark_completed(
    message_id: int,
    odoo_response: str,
    completed_at: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    ack: str | None = None,
    customer_id: int | None = None,
    customer_name: str | None = None,
    operator_id: int | None = None,
    operator_name: str | None = None,
    number_of_operators: int | None = None,
    batch_number: str | None = None,
    machine_id: str | None = None,
) -> None:
    """Mark a queue row COMPLETED and store the returned Odoo response."""
    initialize_database(database_path)

    with _open_connection(database_path) as connection:
        source = connection.execute(
            f"""
            SELECT {_record_columns_sql()}
            FROM mqtt_messages
            WHERE id = ?
            """,
            (message_id,),
        ).fetchone()
        if source is None:
            raise ValueError(f"Message id not found: {message_id}")

        metadata: dict[str, object | None] = {
            "customer_id": None,
            "customer_name": None,
            "operator_id": None,
            "operator_name": None,
            "number_of_operators": None,
            "batch_number": None,
        }
        if ack:
            metadata = _production_metadata_from_response(odoo_response, message_id)
        customer_id = customer_id if customer_id is not None else metadata["customer_id"]
        customer_name = customer_name if customer_name is not None else metadata["customer_name"]
        operator_id = operator_id if operator_id is not None else metadata["operator_id"]
        operator_name = operator_name if operator_name is not None else metadata["operator_name"]
        number_of_operators = (
            number_of_operators
            if number_of_operators is not None
            else metadata["number_of_operators"]
        )
        batch_number = batch_number if batch_number is not None else metadata["batch_number"]

        cursor = connection.execute(
            """
            UPDATE mqtt_messages
            SET status = ?,
                processed_at = ?,
                completed_at = ?,
                last_error = NULL,
                odoo_response = ?,
                ack = ?,
                customer_id = ?,
                customer_name = ?,
                operator_id = ?,
                operator_name = ?,
                batch_number = ?
            WHERE id = ?
            """,
            (
                "COMPLETED",
                completed_at,
                completed_at,
                odoo_response,
                ack,
                customer_id,
                customer_name,
                operator_id,
                operator_name,
                batch_number,
                message_id,
            ),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"Message id not found: {message_id}")

        if ack:
            _upsert_production_record_and_summary(
                connection=connection,
                source=source,
                completed_at=completed_at,
                machine_id=machine_id or DEFAULT_MACHINE_ID,
                ack=ack,
                customer_id=customer_id,
                customer_name=customer_name,
                operator_id=operator_id,
                operator_name=operator_name,
                number_of_operators=number_of_operators,
                batch_number=batch_number,
                raw_odoo_response=odoo_response,
            )


def mark_failed(
    message_id: int,
    error: str,
    last_attempt_at: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    odoo_response: str | None = None,
    *,
    retryable: bool = True,
    max_retries: int | None = None,
) -> None:
    """Mark a queue row FAILED, incrementing retry_count and storing failure detail."""
    initialize_database(database_path)
    retry_count_sql = "retry_count + 1"
    parameters: tuple[object, ...]
    if not retryable:
        final_retry_count = max_retries if max_retries is not None else 10
        retry_count_sql = f"MAX(retry_count + 1, ?)"
        parameters = (
            "FAILED",
            final_retry_count,
            error,
            last_attempt_at,
            odoo_response,
            message_id,
        )
    else:
        parameters = ("FAILED", error, last_attempt_at, odoo_response, message_id)

    with _open_connection(database_path) as connection:
        cursor = connection.execute(
            f"""
            UPDATE mqtt_messages
            SET status = ?,
                retry_count = {retry_count_sql},
                last_error = ?,
                last_attempt_at = ?,
                odoo_response = COALESCE(?, odoo_response)
            WHERE id = ?
            """,
            parameters,
        )
        if cursor.rowcount != 1:
            raise ValueError(f"Message id not found: {message_id}")


def reset_stale_processing(
    stale_before: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    exclude_ids: set[int] | tuple[int, ...] | list[int] = (),
) -> int:
    """Recover stale PROCESSING records left by an interrupted process."""
    initialize_database(database_path)
    recovered_at = _format_timestamp(datetime.now(timezone.utc))
    excluded = tuple(int(value) for value in exclude_ids)
    excluded_sql = ""
    parameters: tuple[object, ...]
    base_parameters: tuple[object, ...] = (
        "FAILED",
        "Recovered stale PROCESSING message after timeout",
        recovered_at,
        "PROCESSING",
        stale_before,
    )
    if excluded:
        placeholders = ",".join("?" for _ in excluded)
        excluded_sql = f" AND id NOT IN ({placeholders})"
        parameters = (*base_parameters, *excluded)
    else:
        parameters = base_parameters

    with _open_connection(database_path) as connection:
        cursor = connection.execute(
            f"""
            UPDATE mqtt_messages
            SET status = ?,
                retry_count = retry_count + 1,
                last_error = ?,
                last_attempt_at = ?
            WHERE status = ?
              AND (last_attempt_at IS NULL OR last_attempt_at < ?)
              {excluded_sql}
            """,
            parameters,
        )
        return int(cursor.rowcount)


def get_message_by_id(
    message_id: int,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> MessageRecord | None:
    """Return one queue record by id."""
    initialize_database(database_path)

    with _open_connection(database_path) as connection:
        row = connection.execute(
            f"""
            SELECT {_record_columns_sql()}
            FROM mqtt_messages
            WHERE id = ?
            """,
            (message_id,),
        ).fetchone()

    return _record_from_row(row) if row is not None else None


def get_message_by_hash(
    message_hash: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> MessageRecord | None:
    """Return one queue record by message hash."""
    initialize_database(database_path)

    with _open_connection(database_path) as connection:
        row = connection.execute(
            f"""
            SELECT {_record_columns_sql()}
            FROM mqtt_messages
            WHERE message_hash = ?
            LIMIT 1
            """,
            (message_hash,),
        ).fetchone()

    return _record_from_row(row) if row is not None else None


def record_ack_replay_attempt(
    message_hash: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    replayed_at: datetime | None = None,
) -> AckReplayAttempt:
    """Atomically apply replay throttling for a completed duplicate ACK."""
    initialize_database(database_path)
    now = replayed_at or datetime.now(timezone.utc)
    now_text = _format_timestamp(now)

    connection = _connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT ack_replay_count, last_ack_replayed_at
            FROM mqtt_messages
            WHERE message_hash = ?
            LIMIT 1
            """,
            (message_hash,),
        ).fetchone()
        if row is None:
            connection.commit()
            raise ValueError(f"Message hash not found: {message_hash}")

        replay_count = int(row["ack_replay_count"] or 0)
        last_replayed_at = _parse_timestamp(row["last_ack_replayed_at"])
        if last_replayed_at is None:
            replay_count = 0
        elif now - last_replayed_at >= ACK_REPLAY_WINDOW:
            replay_count = 0
            last_replayed_at = None

        if replay_count >= ACK_REPLAY_MAX_COUNT:
            connection.commit()
            return AckReplayAttempt(
                allowed=False,
                reason="replay-limit-reached",
                replay_count=replay_count,
                last_ack_replayed_at=row["last_ack_replayed_at"],
            )

        if last_replayed_at is not None and now - last_replayed_at < ACK_REPLAY_MIN_INTERVAL:
            connection.commit()
            return AckReplayAttempt(
                allowed=False,
                reason="minimum-interval",
                replay_count=replay_count,
                last_ack_replayed_at=row["last_ack_replayed_at"],
            )

        new_replay_count = replay_count + 1
        connection.execute(
            """
            UPDATE mqtt_messages
            SET ack_replay_count = ?,
                last_ack_replayed_at = ?
            WHERE message_hash = ?
            """,
            (new_replay_count, now_text, message_hash),
        )
        connection.commit()
        return AckReplayAttempt(
            allowed=True,
            reason="replayed",
            replay_count=new_replay_count,
            last_ack_replayed_at=now_text,
        )
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def get_queue_counts(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> dict[str, int]:
    """Return queue counts grouped by status."""
    initialize_database(database_path)

    with _open_connection(database_path) as connection:
        rows = connection.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM mqtt_messages
            GROUP BY status
            """
        ).fetchall()

    counts = {status: 0 for status in sorted(VALID_STATUSES)}
    counts.update({str(row["status"]): int(row["count"]) for row in rows})
    return counts


def reconcile_completed_production_records(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    machine_id: str = DEFAULT_MACHINE_ID,
    limit: int = 100,
) -> ReconciliationResult:
    """Idempotently summarize bounded completed rows without Odoo or MQTT side effects."""
    initialize_database(database_path)
    if limit <= 0:
        return ReconciliationResult(0, 0, 0, 0)

    scanned_rows = recovered_records = recovered_summaries = skipped_rows = 0
    with _open_connection(database_path) as connection:
        rows = connection.execute(
            f"""
            SELECT {_record_columns_sql("m")}
            FROM mqtt_messages AS m
            LEFT JOIN production_records AS p
              ON p.mqtt_message_id = m.id
            WHERE m.status = 'COMPLETED'
              AND m.completed_at IS NOT NULL
              AND m.odoo_response IS NOT NULL
              AND m.ack IS NOT NULL
              AND (p.id IS NULL OR p.summary_applied = 0)
            ORDER BY m.id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        for row in rows:
            scanned_rows += 1
            try:
                summary_applied_before = _production_summary_applied(
                    connection,
                    int(row["id"]),
                )
                metadata = _production_metadata_from_response(
                    str(row["odoo_response"]),
                    int(row["id"]),
                )
                applied = _upsert_production_record_and_summary(
                    connection=connection,
                    source=row,
                    completed_at=str(row["completed_at"]),
                    machine_id=machine_id,
                    ack=str(row["ack"]),
                    customer_id=metadata["customer_id"],
                    customer_name=metadata["customer_name"],
                    operator_id=metadata["operator_id"],
                    operator_name=metadata["operator_name"],
                    number_of_operators=metadata["number_of_operators"],
                    batch_number=metadata["batch_number"],
                    raw_odoo_response=str(row["odoo_response"]),
                )
                if applied:
                    recovered_summaries += 1
                if not summary_applied_before:
                    recovered_records += 1
            except Exception as exc:
                skipped_rows += 1
                LOGGER.warning(
                    "Skipped production reconciliation for mqtt_message_id=%s error=%s",
                    row["id"],
                    exc,
                )

    if recovered_records or recovered_summaries:
        LOGGER.info(
            "Production reconciliation recovered records=%s summaries=%s scanned=%s skipped=%s",
            recovered_records,
            recovered_summaries,
            scanned_rows,
            skipped_rows,
        )
    return ReconciliationResult(
        scanned_rows=scanned_rows,
        recovered_records=recovered_records,
        recovered_summaries=recovered_summaries,
        skipped_rows=skipped_rows,
    )


def cleanup_raw_operational_data(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    retention_days: int = 30,
    batch_size: int = 1000,
    vacuum_enabled: bool = False,
    now: datetime | None = None,
) -> CleanupResult:
    """Delete only old summarized raw records in bounded batches."""
    initialize_database(database_path)
    if retention_days < 0:
        raise ValueError("retention_days must be >= 0")
    if batch_size <= 0:
        return CleanupResult(0, 0, 0, False)

    cutoff = _format_timestamp((now or datetime.now(timezone.utc)) - timedelta(days=retention_days))
    deleted_mqtt = deleted_api = deleted_production = 0

    try:
        with _open_connection(database_path) as connection:
            completed_ids = [
                int(row["id"])
                for row in connection.execute(
                    """
                    SELECT m.id
                    FROM mqtt_messages AS m
                    JOIN production_records AS p
                      ON p.mqtt_message_id = m.id
                    WHERE m.status = 'COMPLETED'
                      AND m.completed_at < ?
                      AND p.summary_applied = 1
                    ORDER BY m.id ASC
                    LIMIT ?
                    """,
                    (cutoff, batch_size),
                ).fetchall()
            ]
            if completed_ids:
                placeholders = ",".join("?" for _ in completed_ids)
                deleted_mqtt = int(
                    connection.execute(
                        f"DELETE FROM mqtt_messages WHERE id IN ({placeholders})",
                        completed_ids,
                    ).rowcount
                )

            remaining_batch = max(0, batch_size - deleted_mqtt)
            if remaining_batch:
                cursor = connection.execute(
                    """
                    DELETE FROM api_commands
                    WHERE id IN (
                        SELECT id
                        FROM api_commands
                        WHERE status IN ('PUBLISHED', 'FAILED', 'REJECTED', 'DUPLICATE')
                          AND received_at < ?
                        ORDER BY id ASC
                        LIMIT ?
                    )
                    """,
                    (cutoff, remaining_batch),
                )
                deleted_api = int(cursor.rowcount)

            remaining_batch = max(0, batch_size - deleted_mqtt - deleted_api)
            if remaining_batch:
                cursor = connection.execute(
                    """
                    DELETE FROM production_records
                    WHERE id IN (
                        SELECT id
                        FROM production_records
                        WHERE summary_applied = 1
                          AND completed_at < ?
                        ORDER BY id ASC
                        LIMIT ?
                    )
                    """,
                    (cutoff, remaining_batch),
                )
                deleted_production = int(cursor.rowcount)

        vacuum_ran = False
        if vacuum_enabled and (deleted_mqtt or deleted_api or deleted_production):
            with _connect(database_path) as connection:
                connection.execute("VACUUM")
                vacuum_ran = True

        result = CleanupResult(
            deleted_mqtt_messages=deleted_mqtt,
            deleted_api_commands=deleted_api,
            deleted_production_records=deleted_production,
            vacuum_ran=vacuum_ran,
        )
        if result.total_deleted_rows:
            LOGGER.info(
                "SQLite raw cleanup deleted mqtt_messages=%s api_commands=%s production_records=%s vacuum=%s",
                deleted_mqtt,
                deleted_api,
                deleted_production,
                vacuum_ran,
            )
        else:
            LOGGER.debug("SQLite raw cleanup deleted 0 row(s)")
        return result
    except Exception as exc:
        LOGGER.exception("SQLite raw cleanup failed")
        return CleanupResult(0, 0, 0, False, str(exc))


def get_database_status(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    retention_days: int = 30,
    cleanup_enabled: bool = True,
    last_cleanup_at: str | None = None,
    last_cleanup_deleted_rows: int | None = None,
    last_cleanup_error: str | None = None,
) -> dict[str, object]:
    """Return lightweight SQLite size and lifecycle metrics for health reporting."""
    initialize_database(database_path)
    initialize_api_commands_table(database_path)
    db_path = Path(database_path)
    with _open_connection(db_path) as connection:
        page_count = int(connection.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        production_records_count = int(
            connection.execute("SELECT COUNT(*) FROM production_records").fetchone()[0]
        )
        daily_summary_count = int(
            connection.execute("SELECT COUNT(*) FROM daily_production_summary").fetchone()[0]
        )
        oldest_raw_record_at = connection.execute(
            """
            SELECT MIN(value) AS oldest_raw_record_at
            FROM (
                SELECT received_at AS value FROM mqtt_messages
                UNION ALL
                SELECT received_at AS value FROM api_commands
                UNION ALL
                SELECT completed_at AS value FROM production_records
            )
            WHERE value IS NOT NULL
            """
        ).fetchone()["oldest_raw_record_at"]

    return {
        "sqlite_database_size_bytes": db_path.stat().st_size if db_path.exists() else 0,
        "sqlite_page_count": page_count,
        "sqlite_page_size": page_size,
        "sqlite_freelist_count": freelist_count,
        "production_records_count": production_records_count,
        "daily_summary_count": daily_summary_count,
        "oldest_raw_record_at": oldest_raw_record_at,
        "retention_days": retention_days,
        "cleanup_enabled": cleanup_enabled,
        "last_cleanup_at": last_cleanup_at,
        "last_cleanup_deleted_rows": last_cleanup_deleted_rows,
        "last_cleanup_error": last_cleanup_error,
    }


def query_recent_production_records(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    table_no: str | None = None,
    model: str | None = None,
    customer_id: int | None = None,
    batch_number: str | None = None,
    operator_id: int | None = None,
    limit: int = 100,
) -> list[dict[str, object]]:
    initialize_database(database_path)
    where, parameters = _dashboard_filters(
        date_column="completed_at",
        date_from=date_from,
        date_to=date_to,
        table_no=table_no,
        model=model,
        customer_id=customer_id,
        batch_number=batch_number,
        operator_id=operator_id,
    )
    safe_limit = _bounded_limit(limit)
    with _open_connection(database_path) as connection:
        rows = connection.execute(
            f"""
            SELECT id, mqtt_message_id, completed_at, production_date, message_type,
                   machine_id, table_no, model, serial, ack, customer_id, customer_name,
                   operator_id, operator_name, number_of_operators, batch_number, status
            FROM production_records
            {where}
            ORDER BY completed_at DESC, id DESC
            LIMIT ?
            """,
            (*parameters, safe_limit),
        ).fetchall()
    return [dict(row) for row in rows]


def query_daily_production_summary(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    table_no: str | None = None,
    model: str | None = None,
    customer_id: int | None = None,
    batch_number: str | None = None,
    operator_id: int | None = None,
    limit: int = 100,
) -> list[dict[str, object]]:
    initialize_database(database_path)
    where, parameters = _dashboard_filters(
        date_column="production_date",
        date_from=date_from,
        date_to=date_to,
        table_no=table_no,
        model=model,
        customer_id=customer_id,
        batch_number=batch_number,
        operator_id=operator_id,
    )
    safe_limit = _bounded_limit(limit)
    with _open_connection(database_path) as connection:
        rows = connection.execute(
            f"""
            SELECT production_date, machine_id, table_no, model, customer_id,
                   customer_name, batch_number, operator_id, operator_name,
                   number_of_operators, production_count, first_ack, last_ack,
                   first_completed_at, last_completed_at
            FROM daily_production_summary
            {where}
            ORDER BY production_date DESC, table_no, model
            LIMIT ?
            """,
            (*parameters, safe_limit),
        ).fetchall()
    return [dict(row) for row in rows]


def query_production_summary_totals(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    table_no: str | None = None,
    model: str | None = None,
    customer_id: int | None = None,
    batch_number: str | None = None,
    operator_id: int | None = None,
) -> dict[str, object]:
    initialize_database(database_path)
    where, parameters = _dashboard_filters(
        date_column="production_date",
        date_from=date_from,
        date_to=date_to,
        table_no=table_no,
        model=model,
        customer_id=customer_id,
        batch_number=batch_number,
        operator_id=operator_id,
    )
    with _open_connection(database_path) as connection:
        row = connection.execute(
            f"""
            SELECT COALESCE(SUM(production_count), 0) AS production_count,
                   MIN(first_completed_at) AS first_completed_at,
                   MAX(last_completed_at) AS last_completed_at,
                   MIN(production_date) AS first_production_date,
                   MAX(production_date) AS last_production_date
            FROM daily_production_summary
            {where}
            """,
            parameters,
        ).fetchone()
    return dict(row)


def generate_message_hash(topic: str, raw_payload: str) -> str:
    """Generate the unique message identity from topic and raw payload."""
    return hashlib.sha256(f"{topic}{raw_payload}".encode("utf-8")).hexdigest()


def generate_api_payload_hash(payload: str) -> str:
    """Generate the audit hash for an API command payload."""
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _connect(database_path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(Path(database_path), timeout=30, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    return connection


@contextmanager
def _open_connection(database_path: str | Path) -> Iterator[sqlite3.Connection]:
    connection = _connect(database_path)
    try:
        connection.execute("BEGIN")
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _migrate_mqtt_messages(connection: sqlite3.Connection) -> None:
    existing_columns = {
        str(row["name"])
        for row in connection.execute("PRAGMA table_info(mqtt_messages)").fetchall()
    }
    for column_name, definition in MIGRATION_COLUMNS.items():
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE mqtt_messages ADD COLUMN {column_name} {definition}"
            )


def _initialize_production_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS production_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mqtt_message_id INTEGER NOT NULL UNIQUE,
            completed_at TEXT NOT NULL,
            production_date TEXT NOT NULL,
            message_type TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            table_no TEXT NOT NULL,
            model TEXT NOT NULL,
            serial TEXT NOT NULL,
            ack TEXT NOT NULL,
            customer_id INTEGER,
            customer_name TEXT,
            operator_id INTEGER,
            operator_name TEXT,
            number_of_operators INTEGER,
            batch_number TEXT,
            status TEXT NOT NULL,
            raw_odoo_response TEXT NOT NULL,
            created_at TEXT NOT NULL,
            summary_applied INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_production_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            production_date TEXT NOT NULL,
            machine_id TEXT NOT NULL,
            table_no TEXT NOT NULL,
            model TEXT NOT NULL,
            customer_id INTEGER,
            customer_name TEXT,
            batch_number TEXT,
            operator_id INTEGER,
            operator_name TEXT,
            number_of_operators INTEGER,
            production_count INTEGER NOT NULL DEFAULT 0,
            first_ack TEXT,
            last_ack TEXT,
            first_completed_at TEXT,
            last_completed_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_production_records_mqtt_message_id
        ON production_records (mqtt_message_id)
        """
    )
    for index_name, column in (
        ("idx_production_records_completed_at", "completed_at"),
        ("idx_production_records_production_date", "production_date"),
        ("idx_production_records_table_no", "table_no"),
        ("idx_production_records_model", "model"),
        ("idx_production_records_customer_id", "customer_id"),
        ("idx_production_records_batch_number", "batch_number"),
        ("idx_production_records_operator_id", "operator_id"),
    ):
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON production_records ({column})"
        )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_summary_unique_key
        ON daily_production_summary (
            production_date,
            machine_id,
            table_no,
            model,
            COALESCE(customer_id, -1),
            COALESCE(batch_number, ''),
            COALESCE(operator_id, -1)
        )
        """
    )
    for index_name, column in (
        ("idx_daily_summary_production_date", "production_date"),
        ("idx_daily_summary_table_no", "table_no"),
        ("idx_daily_summary_model", "model"),
        ("idx_daily_summary_customer_id", "customer_id"),
        ("idx_daily_summary_batch_number", "batch_number"),
        ("idx_daily_summary_operator_id", "operator_id"),
    ):
        connection.execute(
            f"CREATE INDEX IF NOT EXISTS {index_name} ON daily_production_summary ({column})"
        )


def _production_metadata_from_response(
    odoo_response: str,
    mqtt_message_id: int,
) -> dict[str, object | None]:
    metadata: dict[str, object | None] = {
        "customer_id": None,
        "customer_name": None,
        "operator_id": None,
        "operator_name": None,
        "number_of_operators": None,
        "batch_number": None,
    }
    try:
        parsed = json.loads(odoo_response)
    except json.JSONDecodeError as exc:
        LOGGER.warning(
            "Unable to parse Odoo response metadata for mqtt_message_id=%s: %s",
            mqtt_message_id,
            exc,
        )
        return metadata

    result = parsed.get("result") if isinstance(parsed, dict) else None
    if not isinstance(result, dict):
        LOGGER.warning(
            "Odoo response metadata missing result object for mqtt_message_id=%s",
            mqtt_message_id,
        )
        return metadata

    metadata["customer_id"] = _metadata_int(result.get("customer_id"), "customer_id", mqtt_message_id)
    metadata["customer_name"] = _metadata_str(result.get("customer_name"), "customer_name", mqtt_message_id)
    metadata["operator_id"] = _metadata_int(result.get("operator_id"), "operator_id", mqtt_message_id)
    metadata["operator_name"] = _metadata_str(result.get("operator_name"), "operator_name", mqtt_message_id)
    metadata["number_of_operators"] = _metadata_int(
        result.get("number_of_operators"),
        "number_of_operators",
        mqtt_message_id,
    )
    metadata["batch_number"] = _metadata_str(result.get("batch_number"), "batch_number", mqtt_message_id)
    return metadata


def _metadata_int(value: object, field_name: str, mqtt_message_id: int) -> int | None:
    if value is None:
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    LOGGER.warning(
        "Ignoring invalid Odoo metadata field=%s value=%r mqtt_message_id=%s",
        field_name,
        value,
        mqtt_message_id,
    )
    return None


def _metadata_str(value: object, field_name: str, mqtt_message_id: int) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value
    LOGGER.warning(
        "Ignoring invalid Odoo metadata field=%s value=%r mqtt_message_id=%s",
        field_name,
        value,
        mqtt_message_id,
    )
    return None


def _upsert_production_record_and_summary(
    *,
    connection: sqlite3.Connection,
    source: sqlite3.Row,
    completed_at: str,
    machine_id: str,
    ack: str,
    customer_id: int | None,
    customer_name: str | None,
    operator_id: int | None,
    operator_name: str | None,
    number_of_operators: int | None,
    batch_number: str | None,
    raw_odoo_response: str,
) -> bool:
    production_date = _production_date(completed_at)
    now_text = _format_timestamp(datetime.now(timezone.utc))
    source_id = int(source["id"])
    existing = connection.execute(
        """
        SELECT summary_applied
        FROM production_records
        WHERE mqtt_message_id = ?
        LIMIT 1
        """,
        (source_id,),
    ).fetchone()
    summary_already_applied = bool(existing and int(existing["summary_applied"] or 0))

    connection.execute(
        """
        INSERT INTO production_records (
            mqtt_message_id,
            completed_at,
            production_date,
            message_type,
            machine_id,
            table_no,
            model,
            serial,
            ack,
            customer_id,
            customer_name,
            operator_id,
            operator_name,
            number_of_operators,
            batch_number,
            status,
            raw_odoo_response,
            created_at,
            summary_applied
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(mqtt_message_id) DO UPDATE SET
            completed_at = excluded.completed_at,
            production_date = excluded.production_date,
            message_type = excluded.message_type,
            machine_id = excluded.machine_id,
            table_no = excluded.table_no,
            model = excluded.model,
            serial = excluded.serial,
            ack = excluded.ack,
            customer_id = excluded.customer_id,
            customer_name = excluded.customer_name,
            operator_id = excluded.operator_id,
            operator_name = excluded.operator_name,
            number_of_operators = excluded.number_of_operators,
            batch_number = excluded.batch_number,
            status = excluded.status,
            raw_odoo_response = excluded.raw_odoo_response
        """,
        (
            source_id,
            completed_at,
            production_date,
            str(source["message_type"]),
            machine_id,
            str(source["table_no"]),
            str(source["model"]),
            str(source["serial"]),
            ack,
            customer_id,
            customer_name,
            operator_id,
            operator_name,
            number_of_operators,
            batch_number,
            "COMPLETED",
            raw_odoo_response,
            now_text,
            1 if summary_already_applied else 0,
        ),
    )

    if summary_already_applied:
        return False

    _increment_daily_summary(
        connection=connection,
        production_date=production_date,
        machine_id=machine_id,
        table_no=str(source["table_no"]),
        model=str(source["model"]),
        customer_id=customer_id,
        customer_name=customer_name,
        batch_number=batch_number,
        operator_id=operator_id,
        operator_name=operator_name,
        number_of_operators=number_of_operators,
        ack=ack,
        completed_at=completed_at,
        now_text=now_text,
    )
    connection.execute(
        """
        UPDATE production_records
        SET summary_applied = 1
        WHERE mqtt_message_id = ?
        """,
        (source_id,),
    )
    return True


def _production_summary_applied(
    connection: sqlite3.Connection,
    mqtt_message_id: int,
) -> bool:
    row = connection.execute(
        """
        SELECT summary_applied
        FROM production_records
        WHERE mqtt_message_id = ?
        LIMIT 1
        """,
        (mqtt_message_id,),
    ).fetchone()
    return bool(row and int(row["summary_applied"] or 0))


def _increment_daily_summary(
    *,
    connection: sqlite3.Connection,
    production_date: str,
    machine_id: str,
    table_no: str,
    model: str,
    customer_id: int | None,
    customer_name: str | None,
    batch_number: str | None,
    operator_id: int | None,
    operator_name: str | None,
    number_of_operators: int | None,
    ack: str,
    completed_at: str,
    now_text: str,
) -> None:
    row = connection.execute(
        """
        SELECT id, first_ack, last_ack, first_completed_at, last_completed_at
        FROM daily_production_summary
        WHERE production_date = ?
          AND machine_id = ?
          AND table_no = ?
          AND model = ?
          AND COALESCE(customer_id, ?) = ?
          AND COALESCE(batch_number, ?) = ?
          AND COALESCE(operator_id, ?) = ?
        LIMIT 1
        """,
        (
            production_date,
            machine_id,
            table_no,
            model,
            SUMMARY_INT_SENTINEL,
            customer_id if customer_id is not None else SUMMARY_INT_SENTINEL,
            SUMMARY_TEXT_SENTINEL,
            batch_number if batch_number is not None else SUMMARY_TEXT_SENTINEL,
            SUMMARY_INT_SENTINEL,
            operator_id if operator_id is not None else SUMMARY_INT_SENTINEL,
        ),
    ).fetchone()

    if row is None:
        connection.execute(
            """
            INSERT INTO daily_production_summary (
                production_date,
                machine_id,
                table_no,
                model,
                customer_id,
                customer_name,
                batch_number,
                operator_id,
                operator_name,
                number_of_operators,
                production_count,
                first_ack,
                last_ack,
                first_completed_at,
                last_completed_at,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                production_date,
                machine_id,
                table_no,
                model,
                customer_id,
                customer_name,
                batch_number,
                operator_id,
                operator_name,
                number_of_operators,
                1,
                ack,
                ack,
                completed_at,
                completed_at,
                now_text,
                now_text,
            ),
        )
        return

    summary_id = int(row["id"])
    first_ack = row["first_ack"]
    last_ack = row["last_ack"]
    first_completed_at = str(row["first_completed_at"])
    last_completed_at = str(row["last_completed_at"])
    new_first_ack = ack if completed_at < first_completed_at else first_ack
    new_first_completed_at = (
        completed_at if completed_at < first_completed_at else first_completed_at
    )
    new_last_ack = ack if completed_at >= last_completed_at else last_ack
    new_last_completed_at = (
        completed_at if completed_at >= last_completed_at else last_completed_at
    )

    connection.execute(
        """
        UPDATE daily_production_summary
        SET production_count = production_count + 1,
            first_ack = ?,
            last_ack = ?,
            first_completed_at = ?,
            last_completed_at = ?,
            customer_name = COALESCE(?, customer_name),
            operator_name = COALESCE(?, operator_name),
            number_of_operators = COALESCE(?, number_of_operators),
            updated_at = ?
        WHERE id = ?
        """,
        (
            new_first_ack,
            new_last_ack,
            new_first_completed_at,
            new_last_completed_at,
            customer_name,
            operator_name,
            number_of_operators,
            now_text,
            summary_id,
        ),
    )


def _production_date(completed_at: str) -> str:
    parsed = _parse_timestamp(completed_at)
    if parsed is None:
        raise ValueError("completed_at is required")
    return parsed.date().isoformat()


def _dashboard_filters(
    *,
    date_column: str,
    date_from: str | None,
    date_to: str | None,
    table_no: str | None,
    model: str | None,
    customer_id: int | None,
    batch_number: str | None,
    operator_id: int | None,
) -> tuple[str, tuple[object, ...]]:
    clauses: list[str] = []
    parameters: list[object] = []
    if date_from:
        clauses.append(f"{date_column} >= ?")
        parameters.append(date_from)
    if date_to:
        clauses.append(f"{date_column} <= ?")
        parameters.append(date_to)
    for column, value in (
        ("table_no", table_no),
        ("model", model),
        ("customer_id", customer_id),
        ("batch_number", batch_number),
        ("operator_id", operator_id),
    ):
        if value is not None:
            clauses.append(f"{column} = ?")
            parameters.append(value)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    return where, tuple(parameters)


def _bounded_limit(value: int, *, default: int = 100, maximum: int = 500) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return min(max(parsed, 1), maximum)


def _get_saved_message_by_hash(
    connection: sqlite3.Connection,
    message_hash: str,
) -> SavedMessage | None:
    row = connection.execute(
        """
        SELECT id, message_hash, status
        FROM mqtt_messages
        WHERE message_hash = ?
        LIMIT 1
        """,
        (message_hash,),
    ).fetchone()
    if row is None:
        return None

    return SavedMessage(
        id=int(row["id"]),
        message_hash=str(row["message_hash"]),
        status=str(row["status"]),
        inserted=False,
    )


def _record_from_row(row: sqlite3.Row) -> MessageRecord:
    return MessageRecord(
        id=int(row["id"]),
        received_at=str(row["received_at"]),
        topic=str(row["topic"]),
        message_type=str(row["message_type"]),
        table_no=str(row["table_no"]),
        model=str(row["model"]),
        serial=str(row["serial"]),
        raw_payload=str(row["raw_payload"]),
        message_hash=str(row["message_hash"]),
        status=str(row["status"]),
        retry_count=int(row["retry_count"]),
        processed_at=row["processed_at"],
        last_error=row["last_error"],
        odoo_response=row["odoo_response"],
        last_attempt_at=row["last_attempt_at"],
        completed_at=row["completed_at"],
        ack=row["ack"],
        customer_id=row["customer_id"],
        customer_name=row["customer_name"],
        operator_id=row["operator_id"],
        operator_name=row["operator_name"],
        batch_number=row["batch_number"],
        ack_replay_count=int(row["ack_replay_count"] or 0),
        last_ack_replayed_at=row["last_ack_replayed_at"],
    )


def _api_command_from_row(row: sqlite3.Row) -> ApiCommandRecord:
    return ApiCommandRecord(
        id=int(row["id"]),
        request_id=str(row["request_id"]),
        idempotency_key=row["idempotency_key"],
        received_at=str(row["received_at"]),
        username=row["username"],
        remote_address=row["remote_address"],
        payload=str(row["payload"]),
        payload_hash=str(row["payload_hash"]),
        mqtt_topic=str(row["mqtt_topic"]),
        status=str(row["status"]),
        mqtt_rc=row["mqtt_rc"],
        mqtt_mid=row["mqtt_mid"],
        published_at=row["published_at"],
        response_code=row["response_code"],
        response_body=row["response_body"],
        last_error=row["last_error"],
    )


def _update_api_command_result(
    *,
    request_id: str,
    status: ApiCommandStatusValue,
    mqtt_rc: int | None,
    mqtt_mid: int | None,
    published_at: str | None,
    last_error: str | None,
    database_path: str | Path,
) -> None:
    if status not in API_COMMAND_STATUSES:
        raise ValueError(f"Unsupported API command status: {status}")

    initialize_api_commands_table(database_path)

    with _open_connection(database_path) as connection:
        cursor = connection.execute(
            """
            UPDATE api_commands
            SET status = ?,
                mqtt_rc = ?,
                mqtt_mid = ?,
                published_at = ?,
                last_error = ?
            WHERE request_id = ?
            """,
            (status, mqtt_rc, mqtt_mid, published_at, last_error, request_id),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"API command request_id not found: {request_id}")


def _record_columns_sql(table_alias: str | None = None) -> str:
    columns = (
        "id",
        "received_at",
        "topic",
        "message_type",
        "table_no",
        "model",
        "serial",
        "raw_payload",
        "message_hash",
        "status",
        "retry_count",
        "processed_at",
        "last_error",
        "odoo_response",
        "last_attempt_at",
        "completed_at",
        "ack",
        "customer_id",
        "customer_name",
        "operator_id",
        "operator_name",
        "batch_number",
        "ack_replay_count",
        "last_ack_replayed_at",
    )
    prefix = f"{table_alias}." if table_alias else ""
    return ",\n        ".join(f"{prefix}{column}" for column in columns)


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
