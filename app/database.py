"""SQLite persistence and queue state for received MQTT messages."""

from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal


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
}


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


MqttMessageRecord = MessageRecord


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
    received_at_text = _format_timestamp(received_at or datetime.now(UTC))
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
        published_at=_format_timestamp(published_at or datetime.now(UTC)),
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
    received_at_text = _format_timestamp(received_at or datetime.now(UTC))
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
        processed_at_text = _format_timestamp(datetime.now(UTC))

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

    last_attempt_at = _format_timestamp(datetime.now(UTC))
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
) -> None:
    """Mark a queue row COMPLETED and store the returned Odoo response."""
    initialize_database(database_path)

    with _open_connection(database_path) as connection:
        cursor = connection.execute(
            """
            UPDATE mqtt_messages
            SET status = ?,
                processed_at = ?,
                completed_at = ?,
                last_error = NULL,
                odoo_response = ?
            WHERE id = ?
            """,
            ("COMPLETED", completed_at, completed_at, odoo_response, message_id),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"Message id not found: {message_id}")


def mark_failed(
    message_id: int,
    error: str,
    last_attempt_at: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> None:
    """Mark a queue row FAILED, incrementing retry_count and storing last_error."""
    initialize_database(database_path)

    with _open_connection(database_path) as connection:
        cursor = connection.execute(
            """
            UPDATE mqtt_messages
            SET status = ?,
                retry_count = retry_count + 1,
                last_error = ?,
                last_attempt_at = ?
            WHERE id = ?
            """,
            ("FAILED", error, last_attempt_at, message_id),
        )
        if cursor.rowcount != 1:
            raise ValueError(f"Message id not found: {message_id}")


def reset_stale_processing(
    stale_before: str,
    database_path: str | Path = DEFAULT_DATABASE_PATH,
) -> int:
    """Recover stale PROCESSING records left by an interrupted process."""
    initialize_database(database_path)
    recovered_at = _format_timestamp(datetime.now(UTC))

    with _open_connection(database_path) as connection:
        cursor = connection.execute(
            """
            UPDATE mqtt_messages
            SET status = ?,
                retry_count = retry_count + 1,
                last_error = ?,
                last_attempt_at = ?
            WHERE status = ?
              AND (last_attempt_at IS NULL OR last_attempt_at < ?)
            """,
            (
                "FAILED",
                "Recovered stale PROCESSING message after application restart",
                recovered_at,
                "PROCESSING",
                stale_before,
            ),
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


def _record_columns_sql() -> str:
    return """
        id,
        received_at,
        topic,
        message_type,
        table_no,
        model,
        serial,
        raw_payload,
        message_hash,
        status,
        retry_count,
        processed_at,
        last_error,
        odoo_response,
        last_attempt_at,
        completed_at
    """


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="seconds")
