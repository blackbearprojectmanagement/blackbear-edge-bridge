from __future__ import annotations

import sqlite3
import unittest
import uuid
from contextlib import closing
from pathlib import Path

from app.database import (
    claim_pending_messages,
    generate_message_hash,
    get_message_by_id,
    get_pending_messages,
    initialize_database,
    mark_completed,
    mark_failed,
    message_exists,
    reset_stale_processing,
    save_message,
    update_status,
)


class TestDatabase(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / "data"
        temp_root.mkdir(parents=True, exist_ok=True)
        self.database_path = temp_root / f"test_bridge_{uuid.uuid4().hex}.db"

    def tearDown(self) -> None:
        for path in (
            self.database_path,
            self.database_path.with_name(f"{self.database_path.name}-wal"),
            self.database_path.with_name(f"{self.database_path.name}-shm"),
        ):
            if path.exists():
                path.unlink()

    def test_database_creation(self) -> None:
        initialize_database(self.database_path)

        self.assertTrue(self.database_path.exists())
        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                row = connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table' AND name = 'mqtt_messages'
                    """
                ).fetchone()

        self.assertIsNotNone(row)

    def test_database_migration_adds_missing_columns_and_preserves_data(self) -> None:
        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                connection.execute(
                    """
                    CREATE TABLE mqtt_messages (
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
                    INSERT INTO mqtt_messages (
                        received_at, topic, message_type, table_no, model, serial,
                        raw_payload, message_hash, status
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "2026-07-15T00:00:00+00:00",
                        "MQTT/PLC_TO_ODOO/topic",
                        "MN",
                        "T01",
                        "106-020C012P001",
                        "3241",
                        '{"MN":"106-020C012P0013241T01"}',
                        "old-hash",
                        "NEW",
                    ),
                )

        initialize_database(self.database_path)

        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                columns = {
                    row[1] for row in connection.execute("PRAGMA table_info(mqtt_messages)")
                }
                count = connection.execute("SELECT COUNT(*) FROM mqtt_messages").fetchone()[0]

        self.assertIn("last_error", columns)
        self.assertIn("odoo_response", columns)
        self.assertIn("last_attempt_at", columns)
        self.assertIn("completed_at", columns)
        self.assertEqual(count, 1)

    def test_insert_message(self) -> None:
        saved = self._save_sample_message()

        self.assertTrue(saved.inserted)
        self.assertEqual(saved.status, "NEW")

        pending_messages = get_pending_messages(self.database_path)
        self.assertEqual(len(pending_messages), 1)
        self.assertEqual(pending_messages[0].id, saved.id)
        self.assertEqual(pending_messages[0].message_type, "MN")
        self.assertEqual(pending_messages[0].table_no, "T01")
        self.assertEqual(pending_messages[0].model, "106-020C012P001")
        self.assertEqual(pending_messages[0].serial, "3241")

    def test_duplicate_detection(self) -> None:
        first = self._save_sample_message()
        second = self._save_sample_message()
        expected_hash = generate_message_hash(
            "MQTT/PLC_TO_ODOO/topic",
            '{"MN":"106-020C012P0013241T01"}',
        )

        self.assertTrue(first.inserted)
        self.assertFalse(second.inserted)
        self.assertEqual(first.id, second.id)
        self.assertTrue(message_exists(expected_hash, self.database_path))

        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                count = connection.execute("SELECT COUNT(*) FROM mqtt_messages").fetchone()[0]

        self.assertEqual(count, 1)

    def test_status_update(self) -> None:
        saved = self._save_sample_message()

        update_status(saved.id, "COMPLETED", self.database_path)

        self.assertEqual(get_pending_messages(self.database_path), [])
        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                row = connection.execute(
                    "SELECT status, processed_at FROM mqtt_messages WHERE id = ?",
                    (saved.id,),
                ).fetchone()

        self.assertEqual(row[0], "COMPLETED")
        self.assertIsNotNone(row[1])

    def test_new_row_is_claimed_as_processing(self) -> None:
        saved = self._save_sample_message()

        claimed = claim_pending_messages(10, 10, self.database_path)

        self.assertEqual([record.id for record in claimed], [saved.id])
        self.assertEqual(claimed[0].status, "PROCESSING")
        self.assertIsNotNone(claimed[0].last_attempt_at)

    def test_successful_row_becomes_completed(self) -> None:
        saved = self._save_sample_message()
        claimed = claim_pending_messages(10, 10, self.database_path)[0]

        mark_completed(claimed.id, "{'ok': True}", "2026-07-15T00:00:00+00:00", self.database_path)

        record = get_message_by_id(saved.id, self.database_path)
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "COMPLETED")
        self.assertEqual(record.odoo_response, "{'ok': True}")
        self.assertEqual(record.completed_at, "2026-07-15T00:00:00+00:00")
        self.assertIsNone(record.last_error)

    def test_failed_row_becomes_failed_and_retry_count_increments(self) -> None:
        saved = self._save_sample_message()
        claimed = claim_pending_messages(10, 10, self.database_path)[0]

        mark_failed(claimed.id, "network down", "2026-07-15T00:00:00+00:00", self.database_path)

        record = get_message_by_id(saved.id, self.database_path)
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 1)
        self.assertEqual(record.last_error, "network down")

    def test_max_retry_limit_is_respected(self) -> None:
        saved = self._save_sample_message()
        claimed = claim_pending_messages(10, 10, self.database_path)[0]
        mark_failed(claimed.id, "first failure", "2026-07-15T00:00:00+00:00", self.database_path)

        claimed_again = claim_pending_messages(10, 1, self.database_path)

        self.assertEqual(claimed_again, [])
        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")

    def test_stale_processing_records_are_recovered(self) -> None:
        saved = self._save_sample_message()
        claim_pending_messages(10, 10, self.database_path)

        recovered = reset_stale_processing("2099-01-01T00:00:00+00:00", self.database_path)

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(recovered, 1)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 1)
        self.assertEqual(
            record.last_error,
            "Recovered stale PROCESSING message after application restart",
        )

    def _save_sample_message(self):
        return save_message(
            topic="MQTT/PLC_TO_ODOO/topic",
            raw_payload='{"MN":"106-020C012P0013241T01"}',
            message_type="MN",
            table_no="T01",
            model="106-020C012P001",
            serial="3241",
            database_path=self.database_path,
        )


if __name__ == "__main__":
    unittest.main()
