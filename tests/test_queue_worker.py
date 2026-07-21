from __future__ import annotations

import json
import sqlite3
import threading
import time
import unittest
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from app.database import claim_pending_messages, get_message_by_id, save_message
from app.odoo_client import OdooAuthenticationError, OdooSubmissionError
from app.queue_worker import OdooQueueWorker


DEFAULT_RESPONSE = ("__default_response__",)


class StickyMonotonic:
    def __init__(self, values: list[float]) -> None:
        self._values = values
        self._index = 0

    def __call__(self) -> float:
        if self._index >= len(self._values):
            return self._values[-1]

        value = self._values[self._index]
        self._index += 1
        return value


class FakeOdooClient:
    def __init__(
        self,
        *,
        authenticated: bool = True,
        fail_on: int | None = None,
        response: object = DEFAULT_RESPONSE,
    ) -> None:
        self.authenticated = authenticated
        self.fail_on = fail_on
        self.response = response
        self.authenticate_calls = 0
        self.submitted_payloads: list[dict[str, str]] = []

    def is_authenticated(self) -> bool:
        return self.authenticated

    def authenticate(self) -> int:
        self.authenticate_calls += 1
        if not self.authenticated:
            raise OdooAuthenticationError("auth failed")
        return 7

    def submit_print_data(self, payload: dict[str, str]) -> object:
        self.submitted_payloads.append(payload)
        if self.fail_on == len(self.submitted_payloads):
            raise OdooSubmissionError("submission failed")
        if self.response != DEFAULT_RESPONSE:
            return self.response
        return {"success": True, "result": {"ACK": "3242", "accepted": payload}}


class ConditionalOdooClient(FakeOdooClient):
    def __init__(self, *, hang_serials: set[str], sleep_seconds: float = 30.0) -> None:
        super().__init__()
        self.hang_serials = hang_serials
        self.sleep_seconds = sleep_seconds

    def submit_print_data(self, payload: dict[str, str]) -> object:
        value = next(iter(payload.values()))
        if any(serial in value for serial in self.hang_serials):
            time.sleep(self.sleep_seconds)
        without_table = value[:-3]
        if " " in without_table:
            ack = without_table.rsplit(" ", maxsplit=1)[-1]
        else:
            digits: list[str] = []
            for char in reversed(without_table):
                if not char.isdigit():
                    break
                digits.append(char)
            ack = "".join(reversed(digits))
        return {"success": True, "result": {"ACK": ack}}


class TestOdooQueueWorker(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / "data"
        temp_root.mkdir(parents=True, exist_ok=True)
        self.database_path = temp_root / f"test_worker_{uuid.uuid4().hex}.db"

    def tearDown(self) -> None:
        for path in (
            self.database_path,
            self.database_path.with_name(f"{self.database_path.name}-wal"),
            self.database_path.with_name(f"{self.database_path.name}-shm"),
        ):
            if path.exists():
                path.unlink()

    def test_malformed_raw_json_is_marked_failed(self) -> None:
        saved = self._save_raw("{bad json")
        fake_client = FakeOdooClient()
        worker = self._worker(fake_client)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertIn("Malformed raw JSON", record.last_error)
        self.assertEqual(fake_client.submitted_payloads, [])

    def test_unsupported_message_type_is_marked_failed(self) -> None:
        saved = self._save_raw('{"XX":"106-020C012P001 3242T01"}', message_type="XX")
        fake_client = FakeOdooClient()
        worker = self._worker(fake_client)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertIn("Unsupported message type", record.last_error)
        self.assertEqual(fake_client.submitted_payloads, [])

    def test_one_failed_record_does_not_stop_subsequent_records(self) -> None:
        failed = self._save_raw("{bad json")
        succeeded = self._save_raw(
            '{"MN":"106-020C012P001 3243T01"}',
            serial="3243",
        )
        fake_client = FakeOdooClient()
        worker = self._worker(fake_client)

        worker.run_once()

        failed_record = get_message_by_id(failed.id, self.database_path)
        succeeded_record = get_message_by_id(succeeded.id, self.database_path)
        self.assertEqual(failed_record.status, "FAILED")
        self.assertEqual(succeeded_record.status, "COMPLETED")
        self.assertEqual(
            fake_client.submitted_payloads,
            [{"MN": "106-020C012P001 3243T01"}],
        )

    def test_submission_failure_marks_failed_and_increments_retry_count(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        fake_client = FakeOdooClient(fail_on=1)
        worker = self._worker(fake_client)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 1)
        self.assertIn("submission failed", record.last_error)

    def test_successful_submission_marks_completed(self) -> None:
        saved = self._save_raw('{"MP":"Z106-015C020P001 7084T01"}', message_type="MP")
        fake_client = FakeOdooClient()
        worker = self._worker(fake_client)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "COMPLETED")
        self.assertIsNotNone(record.completed_at)
        self.assertIn("accepted", record.odoo_response)
        self.assertEqual(record.ack, "3242")
        self.assertEqual(
            fake_client.submitted_payloads,
            [{"MP": "Z106-015C020P001 7084T01"}],
        )

    def test_successful_submission_logs_elapsed_timing(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        fake_client = FakeOdooClient(
            response={"success": True, "result": {"ACK": "3242", "ok": True}}
        )
        worker = self._worker(fake_client)
        clock = StickyMonotonic([10.0, 10.0, 10.1, 10.125])

        with (
            patch("app.queue_worker.time.monotonic", side_effect=clock),
            self.assertLogs("app.queue_worker", level="INFO") as logs,
        ):
            worker.run_once()

        log_text = "\n".join(logs.output)
        self.assertIn("Odoo XML-RPC Submission Started", log_text)
        self.assertIn(f"Database ID : {saved.id}", log_text)
        self.assertIn("Message Type: MN", log_text)
        self.assertIn("Model       : 106-020C012P001", log_text)
        self.assertIn("Serial      : 3242", log_text)
        self.assertIn("Table       : T01", log_text)
        self.assertIn("Odoo XML-RPC Submission Finished", log_text)
        self.assertIn(f"Database ID     : {saved.id}", log_text)
        self.assertIn("Elapsed Seconds : 0.125000", log_text)
        self.assertIn(
            'Response        : {"success": true, "result": {"ACK": "3242", "ok": true}}',
            log_text,
        )

    def test_failed_submission_logs_elapsed_timing_before_failure_record(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        fake_client = FakeOdooClient(fail_on=1)
        worker = self._worker(fake_client)
        clock = StickyMonotonic([20.0, 20.0, 20.5])

        with (
            patch("app.queue_worker.time.monotonic", side_effect=clock),
            self.assertLogs("app.queue_worker", level="INFO") as logs,
        ):
            worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        log_text = "\n".join(logs.output)
        self.assertIn("Odoo XML-RPC Submission Started", log_text)
        self.assertIn("Odoo XML-RPC Submission Failed", log_text)
        self.assertIn(f"Database ID     : {saved.id}", log_text)
        self.assertIn("Elapsed Seconds : 0.500000", log_text)
        self.assertIn("Error           : submission failed", log_text)
        self.assertLess(
            log_text.index("Odoo XML-RPC Submission Failed"),
            log_text.index("Odoo Submission Failed"),
        )

    def test_successful_odoo_ack_is_published_after_completed(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3243T01"}', serial="3243")
        fake_client = FakeOdooClient(response={"success": True, "result": {"ACK": "3243"}})
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "COMPLETED")
        self.assertEqual(published_acks, ["3243"])

    def test_missing_ack_does_not_publish(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3243T01"}', serial="3243")
        fake_client = FakeOdooClient(response={"success": True, "result": {}})
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.last_error, "Odoo response success=true but missing or invalid ACK")
        self.assertEqual(published_acks, [])

    def test_success_false_does_not_publish(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3243T01"}', serial="3243")
        fake_client = FakeOdooClient(response={"success": False, "result": {"ACK": "3243"}})
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 1)
        self.assertEqual(record.last_error, "Odoo business operation failed")
        self.assertIn('"success": false', record.odoo_response)
        self.assertEqual(published_acks, [])

    def test_business_failure_with_error_marks_failed_and_stores_response(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        response = {"success": False, "error": "No printer configured for table T01"}
        fake_client = FakeOdooClient(response=response)
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        with self.assertLogs("app.queue_worker", level="INFO") as logs:
            worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 1)
        self.assertEqual(record.last_error, "No printer configured for table T01")
        self.assertEqual(record.odoo_response, '{"success": false, "error": "No printer configured for table T01"}')
        self.assertEqual(published_acks, [])

        log_text = "\n".join(logs.output)
        self.assertIn("Odoo Business Submission Failed", log_text)
        self.assertIn("Retry Count : 1", log_text)
        self.assertIn("Error       : No printer configured for table T01", log_text)
        self.assertIn(f"Response    : {record.odoo_response}", log_text)

    def test_business_failure_without_error_uses_fallback(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        fake_client = FakeOdooClient(response={"success": False})
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.last_error, "Odoo business operation failed")
        self.assertEqual(published_acks, [])

    def test_none_response_marks_failed_without_ack(self) -> None:
        self._assert_invalid_response_marks_failed(None)

    def test_string_response_marks_failed_without_ack(self) -> None:
        self._assert_invalid_response_marks_failed("unexpected")

    def test_empty_dict_response_marks_failed_without_ack(self) -> None:
        self._assert_invalid_response_marks_failed({})

    def test_non_boolean_success_response_marks_failed(self) -> None:
        self._assert_invalid_response_marks_failed({"success": "true"})

    def test_retry_limit_still_respected_for_business_failures(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        fake_client = FakeOdooClient(response={"success": False, "error": "try later"})
        worker = self._worker(fake_client, max_retries=1)

        first_count = worker.run_once()
        second_count = worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 1)
        self.assertEqual(fake_client.submitted_payloads, [{"MN": "106-020C012P001 3242T01"}])

    def test_full_odoo_metadata_is_stored(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3272T01"}', serial="3272")
        response = {
            "success": True,
            "result": {
                "ACK": "3272",
                "customer_id": 145,
                "customer_name": "Mahindra",
                "operator_id": 27,
                "operator_name": "Arun",
                "batch_number": "BATCH-20260720-01",
            },
        }
        fake_client = FakeOdooClient(response=response)
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "COMPLETED")
        self.assertEqual(record.odoo_response, json.dumps(response))
        self.assertEqual(record.ack, "3272")
        self.assertEqual(record.customer_id, 145)
        self.assertEqual(record.customer_name, "Mahindra")
        self.assertEqual(record.operator_id, 27)
        self.assertEqual(record.operator_name, "Arun")
        self.assertEqual(record.batch_number, "BATCH-20260720-01")
        self.assertEqual(published_acks, ["3272"])

    def test_success_true_without_ack_becomes_failed(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        fake_client = FakeOdooClient(response={"success": True, "result": {"ACK": ""}})
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.last_error, "Odoo response success=true but missing or invalid ACK")
        self.assertEqual(published_acks, [])

    def test_deterministic_business_failure_is_not_retried(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        response = {"success": False, "error": "Nothing to check the availability for."}
        fake_client = FakeOdooClient(response=response)
        worker = self._worker(fake_client, max_retries=10)

        first_count = worker.run_once()
        second_count = worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 10)
        self.assertEqual(fake_client.submitted_payloads, [{"MN": "106-020C012P001 3242T01"}])

    def test_hung_odoo_call_times_out_and_child_terminates(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 9001T01"}', serial="9001")
        fake_client = ConditionalOdooClient(hang_serials={"9001"}, sleep_seconds=30)
        published_acks: list[str] = []
        worker = self._worker(
            fake_client,
            ack_publisher=published_acks.append,
            max_retries=10,
            submission_timeout=1,
        )

        with self.assertLogs("app.queue_worker", level="WARNING") as logs:
            worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 10)
        self.assertEqual(
            record.last_error,
            "Odoo timeout; server-side completion unknown; manual verification required",
        )
        self.assertEqual(published_acks, [])
        self.assertIsNone(worker._active_submission_process)
        log_text = "\n".join(logs.output)
        self.assertIn("Odoo submission timeout", log_text)
        self.assertIn("manual verification required", log_text)

    def test_worker_continues_to_next_record_after_timeout(self) -> None:
        timed_out = self._save_raw('{"MN":"106-020C012P001 9002T01"}', serial="9002")
        succeeded = self._save_raw(
            '{"MN":"106-020C012P001 9003T01"}',
            serial="9003",
        )
        fake_client = ConditionalOdooClient(hang_serials={"9002"}, sleep_seconds=30)
        published_acks: list[str] = []
        worker = self._worker(
            fake_client,
            ack_publisher=published_acks.append,
            max_retries=10,
            submission_timeout=1,
        )

        processed = worker.run_once()

        timed_out_record = get_message_by_id(timed_out.id, self.database_path)
        succeeded_record = get_message_by_id(succeeded.id, self.database_path)
        self.assertEqual(processed, 2)
        self.assertEqual(timed_out_record.status, "FAILED")
        self.assertEqual(timed_out_record.retry_count, 10)
        self.assertEqual(succeeded_record.status, "COMPLETED")
        self.assertEqual(succeeded_record.ack, "9003")
        self.assertEqual(published_acks, ["9003"])

    def test_timeout_does_not_retry_or_duplicate_submission(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 9007T01"}', serial="9007")
        fake_client = ConditionalOdooClient(hang_serials={"9007"}, sleep_seconds=30)
        published_acks: list[str] = []
        worker = self._worker(
            fake_client,
            ack_publisher=published_acks.append,
            max_retries=3,
            submission_timeout=1,
        )

        first_count = worker.run_once()
        second_count = worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 0)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 3)
        self.assertEqual(
            fake_client.submitted_payloads,
            [{"MN": "106-020C012P001 9007T01"}],
        )
        self.assertEqual(published_acks, [])

    def test_service_stop_terminates_active_child_and_returns_promptly(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 9004T01"}', serial="9004")
        fake_client = ConditionalOdooClient(hang_serials={"9004"}, sleep_seconds=30)
        worker = self._worker(
            fake_client,
            submission_timeout=10,
            watchdog_interval=1,
        )

        worker.start()
        deadline = time.monotonic() + 5
        while worker.state_snapshot().current_record_id != saved.id:
            if time.monotonic() > deadline:
                self.fail("worker did not start processing the hung record")
            time.sleep(0.05)

        stop_started = time.monotonic()
        worker.stop()
        stop_elapsed = time.monotonic() - stop_started

        self.assertLess(stop_elapsed, 5)
        self.assertFalse(worker.is_running())

    def test_health_is_unhealthy_when_heartbeat_is_stale(self) -> None:
        fake_client = FakeOdooClient()
        worker = self._worker(fake_client, submission_timeout=10)
        worker._state.heartbeat(
            datetime.now(timezone.utc) - timedelta(seconds=120)
        )

        with patch.object(worker, "is_running", return_value=True):
            health = worker.health_snapshot(heartbeat_threshold_seconds=1)

        self.assertTrue(health["worker_running"])
        self.assertFalse(health["worker_healthy"])

    def test_health_is_unhealthy_when_current_record_exceeds_timeout(self) -> None:
        fake_client = FakeOdooClient()
        worker = self._worker(fake_client, submission_timeout=1)
        worker._state.begin_record(
            42,
            datetime.now(timezone.utc) - timedelta(seconds=5),
        )

        with patch.object(worker, "is_running", return_value=True):
            health = worker.health_snapshot(heartbeat_threshold_seconds=30)

        self.assertFalse(health["worker_healthy"])
        self.assertEqual(health["worker_current_message_id"], 42)
        self.assertGreaterEqual(health["worker_current_processing_seconds"], 5)

    def test_health_recovers_after_successful_processing(self) -> None:
        self._save_raw('{"MN":"106-020C012P001 9010T01"}', serial="9010")
        fake_client = FakeOdooClient(
            response={"success": True, "result": {"ACK": "9010"}}
        )
        worker = self._worker(fake_client)

        worker.run_once()

        with patch.object(worker, "is_running", return_value=True):
            health = worker.health_snapshot(heartbeat_threshold_seconds=30)

        self.assertTrue(health["worker_healthy"])
        self.assertIsNone(health["worker_current_message_id"])
        self.assertEqual(health["queue_completed_count"], 1)

    def test_watchdog_remains_active_while_worker_submission_is_blocked(self) -> None:
        stale = self._save_raw('{"MN":"106-020C012P001 9005T01"}', serial="9005")
        self._save_raw('{"MN":"106-020C012P001 9006T01"}', serial="9006")
        claim_pending_messages(1, 10, self.database_path)
        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                connection.execute(
                    """
                    UPDATE mqtt_messages
                    SET last_attempt_at = ?
                    WHERE id = ?
                    """,
                    ("2026-07-21T09:00:00+00:00", stale.id),
                )

        fake_client = FakeOdooClient()
        worker = self._worker(
            fake_client,
            max_retries=1,
            stale_processing_timeout=1,
            watchdog_interval=1,
        )
        blocked = threading.Event()
        original_submit = worker._submit_with_hard_timeout

        def blocked_submission(record, payload):
            blocked.set()
            time.sleep(3)
            return original_submit(record, payload)

        with patch.object(worker, "_submit_with_hard_timeout", side_effect=blocked_submission):
            worker.start()
            self.assertTrue(blocked.wait(timeout=5))
            deadline = time.monotonic() + 5
            while get_message_by_id(stale.id, self.database_path).status != "FAILED":
                if time.monotonic() > deadline:
                    self.fail("watchdog did not recover stale PROCESSING row")
                time.sleep(0.05)
            worker.stop()

        recovered = get_message_by_id(stale.id, self.database_path)
        self.assertEqual(recovered.status, "FAILED")
        self.assertGreaterEqual(recovered.retry_count, 1)

    def test_stale_processing_recovery_runs_during_worker_runtime(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        claim_pending_messages(10, 10, self.database_path)
        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                connection.execute(
                    """
                    UPDATE mqtt_messages
                    SET last_attempt_at = ?
                    WHERE id = ?
                    """,
                    ("2026-07-21T09:00:00+00:00", saved.id),
                )

        fake_client = FakeOdooClient()
        worker = self._worker(fake_client, max_retries=1)
        with patch("app.queue_worker.datetime") as fake_datetime:
            fake_datetime.now.return_value = datetime(2026, 7, 21, 10, 0, 0, tzinfo=timezone.utc)
            fake_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
            worker.recover_stale_processing()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 1)
        self.assertEqual(fake_client.submitted_payloads, [])

    def test_authentication_failure_does_not_claim_rows(self) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        fake_client = FakeOdooClient(authenticated=False)
        worker = self._worker(fake_client)

        processed = worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(processed, 0)
        self.assertEqual(record.status, "NEW")

    def _worker(
        self,
        fake_client: FakeOdooClient,
        ack_publisher=None,
        max_retries: int = 10,
        submission_timeout: int = 15,
        stale_processing_timeout: int = 300,
        watchdog_interval: int = 30,
    ) -> OdooQueueWorker:
        return OdooQueueWorker(
            database_path=self.database_path,
            odoo_client=fake_client,
            worker_interval=1,
            batch_size=10,
            max_retries=max_retries,
            stale_processing_timeout=stale_processing_timeout,
            ack_publisher=ack_publisher,
            submission_timeout=submission_timeout,
            watchdog_interval=watchdog_interval,
        )

    def _assert_invalid_response_marks_failed(self, response: object) -> None:
        saved = self._save_raw('{"MN":"106-020C012P001 3242T01"}')
        fake_client = FakeOdooClient(response=response)
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        worker.run_once()

        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.retry_count, 1)
        self.assertEqual(record.last_error, "Invalid Odoo response: missing success flag")
        self.assertEqual(record.odoo_response, json.dumps(response))
        self.assertEqual(published_acks, [])

    def _save_raw(
        self,
        raw_payload: str,
        *,
        message_type: str = "MN",
        serial: str = "3242",
    ):
        return save_message(
            topic="MQTT/PLC_TO_ODOO/topic",
            raw_payload=raw_payload,
            message_type=message_type,
            table_no="T01",
            model="106-020C012P001",
            serial=serial,
            database_path=self.database_path,
        )


if __name__ == "__main__":
    unittest.main()
