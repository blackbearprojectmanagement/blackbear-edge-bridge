"""Background worker that forwards queued SQLite messages to Odoo."""

from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from app.database import (
    MessageRecord,
    claim_pending_messages,
    mark_completed,
    mark_failed,
    reset_stale_processing,
)
from app.odoo_client import OdooAuthenticationError, OdooSubmissionError, OdooXmlRpcClient


LOGGER = logging.getLogger(__name__)
SUPPORTED_MESSAGE_TYPES = frozenset({"MN", "MP"})
SUPPORTED_TABLES = frozenset({"T01", "T02", "T03"})
STALE_RECOVERY_INTERVAL_SECONDS = 30
NON_RETRYABLE_BUSINESS_ERRORS = frozenset(
    {
        "Nothing to check the availability for.",
    }
)


class OdooQueueWorker:
    """Process persisted MQTT messages and submit them to Odoo in the background."""

    def __init__(
        self,
        database_path: str | Path,
        odoo_client: OdooXmlRpcClient,
        worker_interval: int,
        batch_size: int,
        max_retries: int,
        stale_processing_timeout: int,
        ack_publisher: Callable[[str], bool] | None = None,
    ) -> None:
        self._database_path = Path(database_path)
        self._odoo_client = odoo_client
        self._worker_interval = worker_interval
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._stale_processing_timeout = stale_processing_timeout
        self._ack_publisher = ack_publisher
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._last_stale_recovery_at: datetime | None = None

    def start(self) -> None:
        """Start the queue worker thread once."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                LOGGER.warning("Odoo queue worker is already running")
                return

            stale_before = _timestamp(datetime.now(timezone.utc) - timedelta(
                seconds=self._stale_processing_timeout
            ))
            recovered = reset_stale_processing(stale_before, self._database_path)
            self._last_stale_recovery_at = datetime.now(timezone.utc)
            if recovered:
                LOGGER.warning("Recovered %s stale PROCESSING message(s)", recovered)

            try:
                self._odoo_client.authenticate()
            except OdooAuthenticationError as exc:
                LOGGER.error("Odoo authentication failed at startup: %s", exc)

            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="odoo-queue-worker",
                daemon=False,
            )
            self._thread.start()
            LOGGER.info("Odoo queue worker started")

    def stop(self) -> None:
        """Request shutdown and wait briefly for the worker thread."""
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join()
        LOGGER.info("Odoo queue worker stopped")

    def run_once(self) -> int:
        """Process one worker batch and return the number of records attempted."""
        self._recover_stale_processing_if_due()

        if not self._odoo_client.is_authenticated():
            try:
                self._odoo_client.authenticate()
            except OdooAuthenticationError as exc:
                LOGGER.error("Odoo authentication failed; will retry later: %s", exc)
                return 0

        records = claim_pending_messages(
            self._batch_size,
            self._max_retries,
            self._database_path,
        )
        processed_count = 0
        for record in records:
            processed_count += 1
            self._process_record(record)
        return processed_count

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                LOGGER.exception("Unexpected Odoo queue worker error")

            self._stop_event.wait(self._worker_interval)

    def _process_record(self, record: MessageRecord) -> None:
        last_attempt_at = _timestamp(datetime.now(timezone.utc))
        try:
            payload = payload_from_record(record)
        except ValueError as exc:
            failed_record = self._mark_record_failed(record, str(exc), last_attempt_at)
            LOGGER.error("\n%s", format_failure_log(failed_record, str(exc)))
            return

        LOGGER.info("\n%s", format_xmlrpc_submission_started_log(record))
        start_time = time.monotonic()
        try:
            response = self._odoo_client.submit_print_data(payload)
        except (ValueError, OdooAuthenticationError, OdooSubmissionError) as exc:
            elapsed_seconds = time.monotonic() - start_time
            LOGGER.error(
                "\n%s",
                format_xmlrpc_submission_failed_log(record, elapsed_seconds, str(exc)),
            )
            failed_record = self._mark_record_failed(record, str(exc), last_attempt_at)
            LOGGER.error("\n%s", format_failure_log(failed_record, str(exc)))
            return
        except Exception as exc:
            elapsed_seconds = time.monotonic() - start_time
            LOGGER.error(
                "\n%s",
                format_xmlrpc_submission_failed_log(record, elapsed_seconds, str(exc)),
            )
            failed_record = self._mark_record_failed(record, str(exc), last_attempt_at)
            LOGGER.exception("\n%s", format_failure_log(failed_record, str(exc)))
            return

        elapsed_seconds = time.monotonic() - start_time
        completed_at = _timestamp(datetime.now(timezone.utc))
        response_text = json.dumps(response)
        LOGGER.info(
            "\n%s",
            format_xmlrpc_submission_finished_log(record, elapsed_seconds, response_text),
        )

        business_error = get_odoo_business_error(response)
        if business_error is not None:
            retryable = not is_non_retryable_business_error(business_error)
            failed_record = self._mark_record_failed(
                record,
                business_error,
                last_attempt_at,
                odoo_response=response_text,
                retryable=retryable,
            )
            LOGGER.error(
                "\n%s",
                format_business_failure_log(failed_record, business_error, response_text),
            )
            return

        ack = extract_ack(response)
        if ack is None:
            error = "Odoo response success=true but missing or invalid ACK"
            failed_record = self._mark_record_failed(
                record,
                error,
                last_attempt_at,
                odoo_response=response_text,
                retryable=False,
            )
            LOGGER.error("\n%s", format_failure_log(failed_record, error))
            return

        metadata = extract_dashboard_metadata(response)
        mark_completed(
            record.id,
            response_text,
            completed_at,
            self._database_path,
            ack=ack,
            **metadata,
        )
        LOGGER.info("\n%s", format_success_log(record, response_text))
        self._publish_ack(ack)

    def _publish_ack(self, ack: str) -> None:
        if self._ack_publisher is None:
            LOGGER.warning("ACK %s received from Odoo but no MQTT ACK publisher is configured", ack)
            return

        self._ack_publisher(ack)

    def _recover_stale_processing_if_due(self) -> int:
        now = datetime.now(timezone.utc)
        if (
            self._last_stale_recovery_at is not None
            and now - self._last_stale_recovery_at
            < timedelta(seconds=STALE_RECOVERY_INTERVAL_SECONDS)
        ):
            return 0

        stale_before = _timestamp(
            now - timedelta(seconds=self._stale_processing_timeout)
        )
        recovered = reset_stale_processing(stale_before, self._database_path)
        self._last_stale_recovery_at = now
        LOGGER.info("Runtime stale PROCESSING recovery recovered %s row(s)", recovered)
        return recovered

    def _mark_record_failed(
        self,
        record: MessageRecord,
        error: str,
        last_attempt_at: str,
        odoo_response: str | None = None,
        *,
        retryable: bool = True,
    ) -> MessageRecord:
        mark_failed(
            record.id,
            error,
            last_attempt_at,
            self._database_path,
            odoo_response=odoo_response,
            retryable=retryable,
            max_retries=self._max_retries,
        )
        retry_count = record.retry_count + 1
        if not retryable:
            retry_count = max(retry_count, self._max_retries)
        return replace(
            record,
            status="FAILED",
            retry_count=retry_count,
            last_error=error,
            last_attempt_at=last_attempt_at,
            odoo_response=odoo_response if odoo_response is not None else record.odoo_response,
        )


def payload_from_record(record: MessageRecord) -> dict[str, str]:
    """Validate a stored raw payload and return the exact object for Odoo."""
    try:
        payload: Any = json.loads(record.raw_payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed raw JSON: {exc.msg}") from exc

    if not isinstance(payload, dict):
        raise ValueError("Raw payload must be a JSON object")
    if len(payload) != 1:
        raise ValueError("Raw payload must contain exactly one top-level key")

    message_type, value = next(iter(payload.items()))
    if message_type not in SUPPORTED_MESSAGE_TYPES:
        raise ValueError(f"Unsupported message type for Odoo submission: {message_type}")
    if not isinstance(value, str):
        raise ValueError(f"Payload value for {message_type} must be a string")
    if value[-3:] not in SUPPORTED_TABLES:
        raise ValueError("Payload value must end in T01, T02, or T03")

    return {message_type: value}


def extract_ack(response: object) -> str | None:
    """Return ACK when Odoo response is successful and contains a string ACK."""
    if not isinstance(response, dict):
        return None
    if response.get("success") is not True:
        return None

    result = response.get("result")
    if not isinstance(result, dict):
        return None

    ack = result.get("ACK")
    return ack if isinstance(ack, str) and ack else None


def extract_dashboard_metadata(response: object) -> dict[str, object | None]:
    """Return structured dashboard fields from a successful Odoo response."""
    result = response.get("result") if isinstance(response, dict) else None
    if not isinstance(result, dict):
        return {
            "customer_id": None,
            "customer_name": None,
            "operator_id": None,
            "operator_name": None,
            "batch_number": None,
        }

    return {
        "customer_id": _optional_int(result.get("customer_id")),
        "customer_name": _optional_str(result.get("customer_name")),
        "operator_id": _optional_int(result.get("operator_id")),
        "operator_name": _optional_str(result.get("operator_name")),
        "batch_number": _optional_str(result.get("batch_number")),
    }


def get_odoo_business_error(response: object) -> str | None:
    """Return an error string when an XML-RPC response is not a business success."""
    if not isinstance(response, Mapping):
        return "Invalid Odoo response: missing success flag"

    success = response.get("success")
    if success is True:
        return None
    if success is False:
        error = response.get("error")
        if isinstance(error, str) and error:
            return error
        return "Odoo business operation failed"

    return "Invalid Odoo response: missing success flag"


def is_non_retryable_business_error(error: str) -> bool:
    """Return whether a business failure should be finalized immediately."""
    return error.strip() in NON_RETRYABLE_BUSINESS_ERRORS


def format_xmlrpc_submission_started_log(record: MessageRecord) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Odoo XML-RPC Submission Started",
            f"Database ID : {record.id}",
            f"Message Type: {record.message_type}",
            f"Model       : {record.model}",
            f"Serial      : {record.serial}",
            f"Table       : {record.table_no}",
            "-" * 50,
        ]
    )


def format_xmlrpc_submission_finished_log(
    record: MessageRecord,
    elapsed_seconds: float,
    response_text: str,
) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Odoo XML-RPC Submission Finished",
            f"Database ID     : {record.id}",
            f"Elapsed Seconds : {elapsed_seconds:.6f}",
            f"Response        : {response_text}",
            "-" * 50,
        ]
    )


def format_xmlrpc_submission_failed_log(
    record: MessageRecord,
    elapsed_seconds: float,
    error: str,
) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Odoo XML-RPC Submission Failed",
            f"Database ID     : {record.id}",
            f"Elapsed Seconds : {elapsed_seconds:.6f}",
            f"Error           : {error}",
            "-" * 50,
        ]
    )


def format_business_failure_log(
    record: MessageRecord,
    error: str,
    response_text: str,
) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Odoo Business Submission Failed",
            f"Database ID : {record.id}",
            f"Message Type: {record.message_type}",
            f"Table       : {record.table_no}",
            f"Model       : {record.model}",
            f"Serial      : {record.serial}",
            f"Retry Count : {record.retry_count}",
            f"Error       : {error}",
            f"Response    : {response_text}",
            "Status      : FAILED",
            "-" * 50,
        ]
    )


def format_success_log(record: MessageRecord, response_text: str) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Odoo Submission Completed",
            f"Database ID : {record.id}",
            f"Message Type: {record.message_type}",
            f"Table       : {record.table_no}",
            f"Model       : {record.model}",
            f"Serial      : {record.serial}",
            f"Response    : {response_text}",
            "Status      : COMPLETED",
            "-" * 50,
        ]
    )


def format_failure_log(record: MessageRecord, error: str) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Odoo Submission Failed",
            f"Database ID : {record.id}",
            f"Message Type: {record.message_type}",
            f"Table       : {record.table_no}",
            f"Retry Count : {record.retry_count}",
            f"Error       : {error}",
            "Status      : FAILED",
            "-" * 50,
        ]
    )


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None
