"""SQLite persistence for received MQTT messages."""

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


@dataclass(frozen=True, slots=True)
class SavedMessage:
    id: int
    message_hash: str
    status: str
    inserted: bool


@dataclass(frozen=True, slots=True)
class MqttMessageRecord:
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


def initialize_database(database_path: str | Path = DEFAULT_DATABASE_PATH) -> None:
    """Create the SQLite database and required tables if they do not exist."""
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
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_mqtt_messages_status_id
            ON mqtt_messages (status, id)
            """
        )


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
    db_path = Path(database_path)
    received_at_text = _format_timestamp(received_at or datetime.now(UTC))
    message_hash = generate_message_hash(topic, raw_payload)

    with _open_connection(db_path) as connection:
        existing = _get_saved_message_by_hash(connection, message_hash)
        if existing is not None:
            return SavedMessage(
                id=existing.id,
                message_hash=existing.message_hash,
                status=existing.status,
                inserted=False,
            )

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
) -> list[MqttMessageRecord]:
    """Return NEW messages in insertion order for future processing."""
    initialize_database(database_path)

    with _open_connection(database_path) as connection:
        rows = connection.execute(
            """
            SELECT
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
                processed_at
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
    """Update the processing status for a persisted MQTT message."""
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


def generate_message_hash(topic: str, raw_payload: str) -> str:
    """Generate the unique message identity from topic and raw payload."""
    return hashlib.sha256(f"{topic}{raw_payload}".encode("utf-8")).hexdigest()


def _connect(database_path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(Path(database_path), timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    return connection


@contextmanager
def _open_connection(database_path: str | Path) -> Iterator[sqlite3.Connection]:
    connection = _connect(database_path)
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


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


def _record_from_row(row: sqlite3.Row) -> MqttMessageRecord:
    return MqttMessageRecord(
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
    )


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="seconds")
