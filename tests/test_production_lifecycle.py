from __future__ import annotations

import json
import logging
import sqlite3
import unittest
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.database import (
    cleanup_raw_operational_data,
    claim_pending_messages,
    create_api_command_record,
    get_database_status,
    get_message_by_id,
    initialize_api_commands_table,
    initialize_database,
    mark_api_command_published,
    mark_completed,
    mark_failed,
    query_daily_production_summary,
    query_production_summary_totals,
    query_recent_production_records,
    reconcile_completed_production_records,
    save_message,
)


class TestProductionLifecycle(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / "data"
        temp_root.mkdir(parents=True, exist_ok=True)
        self.database_path = temp_root / f"test_production_{uuid.uuid4().hex}.db"
        initialize_database(self.database_path)
        initialize_api_commands_table(self.database_path)

    def tearDown(self) -> None:
        for path in (
            self.database_path,
            self.database_path.with_name(f"{self.database_path.name}-wal"),
            self.database_path.with_name(f"{self.database_path.name}-shm"),
        ):
            if path.exists():
                path.unlink()

    def test_successful_response_creates_one_production_record_and_summary(self) -> None:
        saved = self._save_and_claim(serial="0029")
        response = self._response(ack="0029")

        mark_completed(
            saved.id,
            json.dumps(response),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            ack="0029",
            machine_id="BEB_BBW_TABLE1",
        )

        production = self._production_rows()
        summary = self._summary_rows()
        self.assertEqual(len(production), 1)
        self.assertEqual(production[0]["mqtt_message_id"], saved.id)
        self.assertEqual(production[0]["customer_name"], "MAGNA")
        self.assertEqual(production[0]["operator_name"], "DILIP")
        self.assertEqual(production[0]["number_of_operators"], 4)
        self.assertEqual(production[0]["raw_odoo_response"], json.dumps(response))
        self.assertEqual(production[0]["summary_applied"], 1)
        self.assertEqual(len(summary), 1)
        self.assertEqual(summary[0]["production_count"], 1)
        self.assertEqual(summary[0]["first_ack"], "0029")
        self.assertEqual(summary[0]["last_ack"], "0029")

    def test_missing_optional_metadata_does_not_fail_completion(self) -> None:
        saved = self._save_and_claim(serial="0030")
        response = {"success": True, "result": {"ACK": "0030"}}

        mark_completed(
            saved.id,
            json.dumps(response),
            "2026-07-21T10:01:00+00:00",
            self.database_path,
            ack="0030",
        )

        record = get_message_by_id(saved.id, self.database_path)
        production = self._production_rows()
        self.assertEqual(record.status, "COMPLETED")
        self.assertEqual(len(production), 1)
        self.assertIsNone(production[0]["customer_id"])
        self.assertEqual(self._summary_rows()[0]["production_count"], 1)

    def test_invalid_optional_metadata_is_ignored_without_breaking_ack_completion(self) -> None:
        saved = self._save_and_claim(serial="0031")
        response = {
            "success": True,
            "result": {
                "ACK": "0031",
                "customer_id": "bad",
                "operator_id": True,
                "number_of_operators": "many",
            },
        }

        with self.assertLogs("app.database", level="WARNING") as logs:
            mark_completed(
                saved.id,
                json.dumps(response),
                "2026-07-21T10:02:00+00:00",
                self.database_path,
                ack="0031",
            )

        self.assertEqual(get_message_by_id(saved.id, self.database_path).ack, "0031")
        self.assertIn("Ignoring invalid Odoo metadata", "\n".join(logs.output))
        production = self._production_rows()[0]
        self.assertIsNone(production["customer_id"])
        self.assertIsNone(production["operator_id"])
        self.assertIsNone(production["number_of_operators"])

    def test_duplicate_completion_does_not_duplicate_record_or_summary_count(self) -> None:
        saved = self._save_and_claim(serial="0032")
        response = self._response(ack="0032")

        for _ in range(2):
            mark_completed(
                saved.id,
                json.dumps(response),
                "2026-07-21T10:03:00+00:00",
                self.database_path,
                ack="0032",
            )

        self.assertEqual(len(self._production_rows()), 1)
        self.assertEqual(self._summary_rows()[0]["production_count"], 1)

    def test_daily_summary_first_last_ack_and_timestamps(self) -> None:
        first = self._save_and_claim(serial="0028")
        second = self._save_and_claim(serial="0033")

        mark_completed(
            second.id,
            json.dumps(self._response(ack="0033")),
            "2026-07-21T10:10:00+00:00",
            self.database_path,
            ack="0033",
        )
        mark_completed(
            first.id,
            json.dumps(self._response(ack="0028")),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            ack="0028",
        )

        summary = self._summary_rows()[0]
        self.assertEqual(summary["production_count"], 2)
        self.assertEqual(summary["first_ack"], "0028")
        self.assertEqual(summary["last_ack"], "0033")
        self.assertEqual(summary["first_completed_at"], "2026-07-21T10:00:00+00:00")
        self.assertEqual(summary["last_completed_at"], "2026-07-21T10:10:00+00:00")

    def test_different_summary_keys_create_separate_rows(self) -> None:
        first = self._save_and_claim(serial="0034", table_no="T01")
        second = self._save_and_claim(serial="0035", table_no="T02")

        mark_completed(
            first.id,
            json.dumps(self._response(ack="0034", customer_id=21)),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            ack="0034",
        )
        mark_completed(
            second.id,
            json.dumps(self._response(ack="0035", customer_id=22, customer_name="ALT")),
            "2026-07-21T10:01:00+00:00",
            self.database_path,
            ack="0035",
        )

        self.assertEqual(len(self._summary_rows()), 2)

    def test_reconciliation_repairs_unsummarized_completed_row(self) -> None:
        saved = self._save_and_claim(serial="0036")
        response = json.dumps(self._response(ack="0036"))
        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                connection.execute(
                    """
                    UPDATE mqtt_messages
                    SET status = 'COMPLETED',
                        completed_at = ?,
                        processed_at = ?,
                        odoo_response = ?,
                        ack = ?
                    WHERE id = ?
                    """,
                    (
                        "2026-07-21T11:00:00+00:00",
                        "2026-07-21T11:00:00+00:00",
                        response,
                        "0036",
                        saved.id,
                    ),
                )

        result = reconcile_completed_production_records(
            self.database_path,
            machine_id="BEB_BBW_TABLE1",
            limit=10,
        )

        self.assertEqual(result.recovered_summaries, 1)
        self.assertEqual(len(self._production_rows()), 1)
        self.assertEqual(self._summary_rows()[0]["production_count"], 1)

    def test_reconciliation_is_idempotent(self) -> None:
        self.test_reconciliation_repairs_unsummarized_completed_row()
        result = reconcile_completed_production_records(self.database_path, limit=10)
        self.assertEqual(result.recovered_summaries, 0)
        self.assertEqual(self._summary_rows()[0]["production_count"], 1)

    def test_cleanup_deletes_only_eligible_summarized_terminal_rows(self) -> None:
        old_completed = self._save_and_claim(serial="0040")
        mark_completed(
            old_completed.id,
            json.dumps(self._response(ack="0040")),
            "2026-06-01T10:00:00+00:00",
            self.database_path,
            ack="0040",
        )
        processing = self._save_raw(serial="0042")
        claim_pending_messages(1, 10, self.database_path)
        failed = self._save_raw(serial="0043")
        claimed_failed = claim_pending_messages(10, 10, self.database_path)[0]
        mark_failed(
            claimed_failed.id,
            "temporary",
            "2026-06-01T10:00:00+00:00",
            self.database_path,
            retryable=True,
            max_retries=10,
        )
        new_row = self._save_raw(serial="0041")
        api = create_api_command_record(
            request_id=str(uuid.uuid4()),
            idempotency_key=str(uuid.uuid4()),
            username="odoo",
            remote_address="127.0.0.1",
            payload="{}",
            mqtt_topic="MQTT/ODOO_TO_PLC/topic",
            database_path=self.database_path,
            received_at=datetime(2026, 6, 1, 10, 0, 0, tzinfo=timezone.utc),
        )
        mark_api_command_published(api.request_id, 0, 1, self.database_path)

        result = cleanup_raw_operational_data(
            self.database_path,
            retention_days=30,
            batch_size=1000,
            now=datetime(2026, 7, 21, 10, 0, 0, tzinfo=timezone.utc),
        )

        self.assertGreaterEqual(result.total_deleted_rows, 3)
        self.assertIsNone(get_message_by_id(old_completed.id, self.database_path))
        self.assertIsNotNone(get_message_by_id(new_row.id, self.database_path))
        self.assertIsNotNone(get_message_by_id(processing.id, self.database_path))
        self.assertIsNotNone(get_message_by_id(failed.id, self.database_path))
        self.assertEqual(len(self._summary_rows()), 1)

    def test_cleanup_never_deletes_unsummarized_production_records(self) -> None:
        saved = self._save_and_claim(serial="0044")
        mark_completed(
            saved.id,
            json.dumps(self._response(ack="0044")),
            "2026-06-01T10:00:00+00:00",
            self.database_path,
            ack="0044",
        )
        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                connection.execute(
                    "UPDATE production_records SET summary_applied = 0 WHERE mqtt_message_id = ?",
                    (saved.id,),
                )

        cleanup_raw_operational_data(
            self.database_path,
            retention_days=30,
            batch_size=1000,
            now=datetime(2026, 7, 21, 10, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(len(self._production_rows()), 1)
        self.assertIsNotNone(get_message_by_id(saved.id, self.database_path))

    def test_cleanup_batching_and_zero_delete_logging(self) -> None:
        for serial in ("0045", "0046"):
            saved = self._save_and_claim(serial=serial)
            mark_completed(
                saved.id,
                json.dumps(self._response(ack=serial)),
                "2026-06-01T10:00:00+00:00",
                self.database_path,
                ack=serial,
            )

        first = cleanup_raw_operational_data(
            self.database_path,
            retention_days=30,
            batch_size=1,
            now=datetime(2026, 7, 21, 10, 0, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(first.total_deleted_rows, 1)

        cleanup_raw_operational_data(
            self.database_path,
            retention_days=30,
            batch_size=100,
            now=datetime(2026, 7, 21, 10, 0, 0, tzinfo=timezone.utc),
        )
        with self.assertLogs("app.database", level="DEBUG") as logs:
            cleanup_raw_operational_data(
                self.database_path,
                retention_days=30,
                batch_size=100,
                now=datetime(2026, 7, 21, 10, 0, 0, tzinfo=timezone.utc),
            )
        self.assertIn("deleted 0 row", "\n".join(logs.output))

    def test_dashboard_queries_and_database_status(self) -> None:
        saved = self._save_and_claim(serial="0047")
        mark_completed(
            saved.id,
            json.dumps(self._response(ack="0047")),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            ack="0047",
        )

        self.assertEqual(len(query_recent_production_records(self.database_path)), 1)
        self.assertEqual(len(query_daily_production_summary(self.database_path)), 1)
        totals = query_production_summary_totals(self.database_path)
        self.assertEqual(totals["production_count"], 1)
        status = get_database_status(
            self.database_path,
            retention_days=30,
            cleanup_enabled=True,
            last_cleanup_at="2026-07-21T10:00:00+00:00",
            last_cleanup_deleted_rows=0,
        )
        self.assertGreater(status["sqlite_database_size_bytes"], 0)
        self.assertEqual(status["production_records_count"], 1)
        self.assertEqual(status["daily_summary_count"], 1)
        self.assertEqual(status["retention_days"], 30)

    def _save_and_claim(self, *, serial: str, table_no: str = "T01"):
        saved = self._save_raw(serial=serial, table_no=table_no)
        claim_pending_messages(10, 10, self.database_path)
        return saved

    def _save_raw(self, *, serial: str, table_no: str = "T01"):
        return save_message(
            topic="MQTT/PLC_TO_ODOO/topic",
            raw_payload=json.dumps({"MN": f"106-020C012P001 {serial}{table_no}"}),
            message_type="MN",
            table_no=table_no,
            model="106-020C012P001",
            serial=serial,
            database_path=self.database_path,
        )

    def _response(
        self,
        *,
        ack: str,
        customer_id: int = 21,
        customer_name: str = "MAGNA",
    ) -> dict[str, object]:
        return {
            "success": True,
            "result": {
                "ACK": ack,
                "customer_id": customer_id,
                "customer_name": customer_name,
                "operator_id": 4,
                "operator_name": "DILIP",
                "batch_number": "MS20260721A",
                "number_of_operators": 4,
            },
        }

    def _production_rows(self) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(
                "SELECT * FROM production_records ORDER BY id"
            ).fetchall()

    def _summary_rows(self) -> list[sqlite3.Row]:
        with closing(sqlite3.connect(self.database_path)) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(
                "SELECT * FROM daily_production_summary ORDER BY id"
            ).fetchall()


if __name__ == "__main__":
    unittest.main()
