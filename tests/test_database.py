from __future__ import annotations

import sqlite3
import unittest
import uuid
from contextlib import closing
from pathlib import Path

from app.database import (
    generate_message_hash,
    get_pending_messages,
    initialize_database,
    message_exists,
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
