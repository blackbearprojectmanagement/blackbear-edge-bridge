from __future__ import annotations

import unittest
import uuid
from pathlib import Path

from app.database import get_message_by_id, save_message
from app.odoo_client import OdooAuthenticationError, OdooSubmissionError
from app.queue_worker import OdooQueueWorker


class FakeOdooClient:
    def __init__(
        self,
        *,
        authenticated: bool = True,
        fail_on: int | None = None,
        response: object | None = None,
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
        if self.response is not None:
            return self.response
        return {"accepted": payload}


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
        self.assertEqual(
            fake_client.submitted_payloads,
            [{"MP": "Z106-015C020P001 7084T01"}],
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
        self._save_raw('{"MN":"106-020C012P001 3243T01"}', serial="3243")
        fake_client = FakeOdooClient(response={"success": True, "result": {}})
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        worker.run_once()

        self.assertEqual(published_acks, [])

    def test_success_false_does_not_publish(self) -> None:
        self._save_raw('{"MN":"106-020C012P001 3243T01"}', serial="3243")
        fake_client = FakeOdooClient(response={"success": False, "result": {"ACK": "3243"}})
        published_acks: list[str] = []
        worker = self._worker(fake_client, ack_publisher=published_acks.append)

        worker.run_once()

        self.assertEqual(published_acks, [])

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
    ) -> OdooQueueWorker:
        return OdooQueueWorker(
            database_path=self.database_path,
            odoo_client=fake_client,
            worker_interval=1,
            batch_size=10,
            max_retries=10,
            stale_processing_timeout=300,
            ack_publisher=ack_publisher,
        )

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
