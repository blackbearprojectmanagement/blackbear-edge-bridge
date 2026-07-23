"""Background worker that forwards queued SQLite messages to Odoo."""

from __future__ import annotations

import json
import logging
import multiprocessing
import pickle
import threading
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from app.database import (
    MessageRecord,
    claim_pending_messages,
    get_queue_counts,
    mark_completed,
    mark_failed,
    reset_stale_processing,
)
from app.odoo_client import (
    OdooAuthenticationError,
    OdooClientSettings,
    OdooXmlRpcClient,
)


LOGGER = logging.getLogger(__name__)
SUPPORTED_MESSAGE_TYPES = frozenset({"MN", "MP"})
SUPPORTED_TABLES = frozenset({"T01", "T02", "T03"})
STALE_RECOVERY_INTERVAL_SECONDS = 30
STOP_JOIN_TIMEOUT_SECONDS = 5
CHILD_TERMINATE_JOIN_SECONDS = 2
NON_RETRYABLE_BUSINESS_ERRORS = frozenset(
    {
        "Nothing to check the availability for.",
    }
)


@dataclass(frozen=True, slots=True)
class SubmissionResult:
    response: object | None
    error: str | None
    error_type: str | None
    timed_out: bool
    elapsed_seconds: float
    terminated: bool = False
    killed: bool = False
    exitcode: int | None = None


@dataclass(frozen=True, slots=True)
class WorkerStateSnapshot:
    worker_thread_alive: bool
    watchdog_thread_alive: bool
    last_loop_timestamp: str | None
    current_record_id: int | None
    current_record_start_timestamp: str | None
    current_processing_seconds: float | None
    last_successful_completion_timestamp: str | None
    last_failure_timestamp: str | None
    pending_queue_count: int


class WorkerState:
    """Thread-safe heartbeat and current-record state for health reporting."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last_loop_at: datetime | None = None
        self._current_record_id: int | None = None
        self._current_record_started_at: datetime | None = None
        self._last_success_at: datetime | None = None
        self._last_failure_at: datetime | None = None
        self._pending_queue_count = 0

    def heartbeat(self, when: datetime | None = None) -> None:
        with self._lock:
            self._last_loop_at = when or datetime.now(timezone.utc)

    def begin_record(self, record_id: int, when: datetime | None = None) -> None:
        with self._lock:
            now = when or datetime.now(timezone.utc)
            self._last_loop_at = now
            self._current_record_id = record_id
            self._current_record_started_at = now

    def finish_record(self, *, succeeded: bool, when: datetime | None = None) -> None:
        with self._lock:
            now = when or datetime.now(timezone.utc)
            self._last_loop_at = now
            if succeeded:
                self._last_success_at = now
            else:
                self._last_failure_at = now
            self._current_record_id = None
            self._current_record_started_at = None

    def set_pending_queue_count(self, count: int) -> None:
        with self._lock:
            self._pending_queue_count = count

    def snapshot(
        self,
        *,
        worker_thread_alive: bool,
        watchdog_thread_alive: bool,
        now: datetime | None = None,
    ) -> WorkerStateSnapshot:
        now = now or datetime.now(timezone.utc)
        with self._lock:
            current_seconds = None
            if self._current_record_started_at is not None:
                current_seconds = max(
                    0.0,
                    (now - self._current_record_started_at).total_seconds(),
                )

            return WorkerStateSnapshot(
                worker_thread_alive=worker_thread_alive,
                watchdog_thread_alive=watchdog_thread_alive,
                last_loop_timestamp=_timestamp_optional(self._last_loop_at),
                current_record_id=self._current_record_id,
                current_record_start_timestamp=_timestamp_optional(
                    self._current_record_started_at
                ),
                current_processing_seconds=current_seconds,
                last_successful_completion_timestamp=_timestamp_optional(
                    self._last_success_at
                ),
                last_failure_timestamp=_timestamp_optional(self._last_failure_at),
                pending_queue_count=self._pending_queue_count,
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
        submission_timeout: int | None = None,
        watchdog_interval: int = STALE_RECOVERY_INTERVAL_SECONDS,
    ) -> None:
        self._database_path = Path(database_path)
        self._odoo_client = odoo_client
        self._worker_interval = worker_interval
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._stale_processing_timeout = stale_processing_timeout
        self._ack_publisher = ack_publisher
        self._submission_timeout = submission_timeout or int(
            getattr(odoo_client, "timeout", 15)
        )
        self._watchdog_interval = watchdog_interval
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._state = WorkerState()
        self._active_submission_lock = threading.Lock()
        self._active_submission_process: multiprocessing.Process | None = None
        self._active_submission_record_id: int | None = None
        self._active_submission_started_at: datetime | None = None

    def start(self) -> None:
        """Start the queue worker thread once."""
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                LOGGER.warning("Odoo queue worker is already running")
                return

            try:
                self._odoo_client.authenticate()
            except OdooAuthenticationError as exc:
                LOGGER.error("Odoo authentication failed at startup: %s", exc)

            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="odoo-queue-worker",
                daemon=True,
            )
            self._watchdog_thread = threading.Thread(
                target=self._watchdog_loop,
                name="odoo-stale-watchdog",
                daemon=True,
            )
            self._thread.start()
            self._watchdog_thread.start()
            LOGGER.info("Odoo queue worker started")

    def stop(self) -> None:
        """Request shutdown and return within a bounded time."""
        self._stop_event.set()
        self._terminate_active_submission("worker shutdown")
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=STOP_JOIN_TIMEOUT_SECONDS)
            if thread.is_alive():
                LOGGER.warning(
                    "Odoo queue worker thread did not stop within %s seconds",
                    STOP_JOIN_TIMEOUT_SECONDS,
                )
        watchdog_thread = self._watchdog_thread
        if watchdog_thread is not None and watchdog_thread.is_alive():
            watchdog_thread.join(timeout=STOP_JOIN_TIMEOUT_SECONDS)
            if watchdog_thread.is_alive():
                LOGGER.warning(
                    "Odoo stale watchdog thread did not stop within %s seconds",
                    STOP_JOIN_TIMEOUT_SECONDS,
                )
        LOGGER.info("Odoo queue worker stopped")

    def run_once(self) -> int:
        """Process one worker batch and return the number of records attempted."""
        self._state.heartbeat()
        self._state.set_pending_queue_count(
            get_queue_counts(self._database_path).get("NEW", 0)
        )

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
        self._state.set_pending_queue_count(
            get_queue_counts(self._database_path).get("NEW", 0)
        )
        processed_count = 0
        for record in records:
            if self._stop_event.is_set():
                break
            processed_count += 1
            self._process_record(record)
        return processed_count

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._state.heartbeat()
            try:
                self.run_once()
            except Exception:
                LOGGER.exception("Unexpected Odoo queue worker error")

            self._stop_event.wait(self._worker_interval)

    def _watchdog_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.recover_stale_processing()
            except Exception:
                LOGGER.exception("Unexpected stale PROCESSING watchdog error")

            self._stop_event.wait(self._watchdog_interval)

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def state_snapshot(self) -> WorkerStateSnapshot:
        return self._state.snapshot(
            worker_thread_alive=self.is_running(),
            watchdog_thread_alive=(
                self._watchdog_thread is not None
                and self._watchdog_thread.is_alive()
            ),
        )

    def health_snapshot(self, heartbeat_threshold_seconds: int) -> dict[str, object]:
        state = self.state_snapshot()
        queue_counts = get_queue_counts(self._database_path)
        now = datetime.now(timezone.utc)
        last_heartbeat = _parse_timestamp(state.last_loop_timestamp)
        heartbeat_age = (
            None
            if last_heartbeat is None
            else (now - last_heartbeat).total_seconds()
        )
        worker_healthy = bool(state.worker_thread_alive)
        if heartbeat_age is None or heartbeat_age > heartbeat_threshold_seconds:
            worker_healthy = False
        if (
            state.current_processing_seconds is not None
            and state.current_processing_seconds > self._submission_timeout
        ):
            worker_healthy = False

        return {
            "worker_running": state.worker_thread_alive,
            "worker_healthy": worker_healthy,
            "worker_last_heartbeat": state.last_loop_timestamp,
            "worker_current_message_id": state.current_record_id,
            "worker_current_processing_seconds": state.current_processing_seconds,
            "queue_new_count": queue_counts.get("NEW", 0),
            "queue_processing_count": queue_counts.get("PROCESSING", 0),
            "queue_failed_count": queue_counts.get("FAILED", 0),
            "queue_completed_count": queue_counts.get("COMPLETED", 0),
        }

    def _process_record(self, record: MessageRecord) -> None:
        last_attempt_at = _timestamp(datetime.now(timezone.utc))
        self._state.begin_record(record.id)
        try:
            payload = payload_from_record(record)
        except ValueError as exc:
            failed_record = self._mark_record_failed(record, str(exc), last_attempt_at)
            LOGGER.error("\n%s", format_failure_log(failed_record, str(exc)))
            self._state.finish_record(succeeded=False)
            return

        LOGGER.info("\n%s", format_xmlrpc_submission_started_log(record))
        start_time = time.monotonic()
        submission = self._submit_with_hard_timeout(record, payload)
        if submission.timed_out:
            # The child is killed locally, but Odoo may still finish the request
            # server-side. MN/MP submissions can produce labels, so a timeout is
            # terminal and requires manual verification rather than automatic retry.
            error = "Odoo timeout; server-side completion unknown; manual verification required"
            LOGGER.error(
                "\n%s",
                format_xmlrpc_submission_timeout_log(
                    record,
                    submission.elapsed_seconds,
                    self._submission_timeout,
                    submission.terminated,
                    submission.killed,
                    submission.exitcode,
                ),
            )
            failed_record = self._mark_record_failed(
                record,
                error,
                last_attempt_at,
                retryable=False,
            )
            LOGGER.error("\n%s", format_failure_log(failed_record, error))
            self._state.finish_record(succeeded=False)
            return

        if submission.error is not None:
            error = submission.error
            LOGGER.error(
                "\n%s",
                format_xmlrpc_submission_failed_log(
                    record,
                    submission.elapsed_seconds,
                    error,
                ),
            )
            failed_record = self._mark_record_failed(record, error, last_attempt_at)
            LOGGER.error("\n%s", format_failure_log(failed_record, error))
            self._state.finish_record(succeeded=False)
            return

        elapsed_seconds = time.monotonic() - start_time
        completed_at = _timestamp(datetime.now(timezone.utc))
        response = submission.response
        response_text = json.dumps(response)
        LOGGER.info(
            "\n%s",
            format_xmlrpc_submission_finished_log(
                record,
                elapsed_seconds,
                response_text,
            ),
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
            self._state.finish_record(succeeded=False)
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
            self._state.finish_record(succeeded=False)
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
        self._state.finish_record(succeeded=True)

    def _publish_ack(self, ack: str) -> None:
        if self._ack_publisher is None:
            LOGGER.warning("ACK %s received from Odoo but no MQTT ACK publisher is configured", ack)
            return

        self._ack_publisher(ack)

    def recover_stale_processing(self) -> int:
        """Recover stale PROCESSING rows from the independent watchdog thread."""
        now = datetime.now(timezone.utc)
        stale_before = _timestamp(
            now - timedelta(seconds=self._stale_processing_timeout)
        )
        excluded_ids = self._active_record_ids_protected_from_watchdog(now)
        recovered = reset_stale_processing(
            stale_before,
            self._database_path,
            exclude_ids=excluded_ids,
        )
        if recovered:
            LOGGER.warning(
                "Stale PROCESSING watchdog recovered %s row(s); excluded active ids=%s",
                recovered,
                sorted(excluded_ids),
            )
        else:
            LOGGER.debug("Stale PROCESSING watchdog recovered 0 row(s)")
        return recovered

    def _active_record_ids_protected_from_watchdog(
        self,
        now: datetime,
    ) -> set[int]:
        with self._active_submission_lock:
            process = self._active_submission_process
            record_id = self._active_submission_record_id
            started_at = self._active_submission_started_at
            if process is None or record_id is None or started_at is None:
                return set()
            if not process.is_alive():
                return set()
            if (now - started_at).total_seconds() > self._submission_timeout:
                return set()
            return {record_id}

    def _submit_with_hard_timeout(
        self,
        record: MessageRecord,
        payload: dict[str, str],
    ) -> SubmissionResult:
        result_path = self._submission_result_path(record.id)
        client_spec = self._submission_client_spec()
        process = multiprocessing.Process(
            target=_submit_payload_in_child,
            args=(client_spec, payload, result_path),
            name=f"odoo-submit-{record.id}",
            daemon=True,
        )
        started_at = datetime.now(timezone.utc)
        start_time = time.monotonic()
        with self._active_submission_lock:
            self._active_submission_process = process
            self._active_submission_record_id = record.id
            self._active_submission_started_at = started_at

        process.start()
        process.join(timeout=self._submission_timeout)
        if process.is_alive():
            LOGGER.warning(
                "Odoo submission timeout for record id=%s serial=%s after %.3fs "
                "(threshold=%ss). Terminating child; manual verification required. "
                "Automatic retry is suppressed because server-side completion is unknown.",
                record.id,
                record.serial,
                time.monotonic() - start_time,
                self._submission_timeout,
            )
            terminated, killed, exitcode = self._terminate_process(process)
            _remove_child_result(result_path)
            self._reset_odoo_client_after_unsafe_transport()
            self._clear_active_submission(process)
            if hasattr(self._odoo_client, "submitted_payloads"):
                self._record_parent_fake_submission(payload)
            return SubmissionResult(
                response=None,
                error=None,
                error_type="timeout",
                timed_out=True,
                elapsed_seconds=time.monotonic() - start_time,
                terminated=terminated,
                killed=killed,
                exitcode=exitcode,
            )

        self._clear_active_submission(process)
        if hasattr(self._odoo_client, "submitted_payloads"):
            self._record_parent_fake_submission(payload)

        child_result = _read_child_result(result_path)
        _remove_child_result(result_path)
        if child_result is None:
            self._reset_odoo_client_after_unsafe_transport()
            return SubmissionResult(
                response=None,
                error=f"Odoo submission child exited without a result (exitcode={process.exitcode})",
                error_type="child-exit",
                timed_out=False,
                elapsed_seconds=time.monotonic() - start_time,
                exitcode=process.exitcode,
            )

        if child_result.get("ok") is True:
            return SubmissionResult(
                response=child_result.get("response"),
                error=None,
                error_type=None,
                timed_out=False,
                elapsed_seconds=time.monotonic() - start_time,
                exitcode=process.exitcode,
            )

        error_type = str(child_result.get("error_type") or "error")
        error = str(child_result.get("error") or "Odoo submission failed")
        if error_type in {"OdooSubmissionError", "OdooAuthenticationError"}:
            self._reset_odoo_client_after_unsafe_transport()
        return SubmissionResult(
            response=None,
            error=error,
            error_type=error_type,
            timed_out=False,
            elapsed_seconds=time.monotonic() - start_time,
            exitcode=process.exitcode,
        )

    def _submission_client_spec(self) -> OdooClientSettings | object:
        settings = getattr(self._odoo_client, "settings", None)
        if callable(settings):
            return settings()
        return self._odoo_client

    def _submission_result_path(self, record_id: int) -> Path:
        result_dir = self._database_path.parent / "worker-results"
        result_dir.mkdir(parents=True, exist_ok=True)
        return result_dir / f"odoo-submit-{record_id}-{uuid.uuid4().hex}.pickle"

    def _terminate_active_submission(self, reason: str) -> None:
        with self._active_submission_lock:
            process = self._active_submission_process
            record_id = self._active_submission_record_id
        if process is None or not process.is_alive():
            return

        LOGGER.warning(
            "Terminating active Odoo submission child for record id=%s during %s",
            record_id,
            reason,
        )
        self._terminate_process(process)
        self._reset_odoo_client_after_unsafe_transport()
        self._clear_active_submission(process)

    def _terminate_process(
        self,
        process: multiprocessing.Process,
    ) -> tuple[bool, bool, int | None]:
        terminated = False
        killed = False
        if process.is_alive():
            process.terminate()
            terminated = True
            process.join(timeout=CHILD_TERMINATE_JOIN_SECONDS)
        if process.is_alive():
            process.kill()
            killed = True
            process.join(timeout=CHILD_TERMINATE_JOIN_SECONDS)
        return terminated, killed, process.exitcode

    def _clear_active_submission(self, process: multiprocessing.Process) -> None:
        with self._active_submission_lock:
            if self._active_submission_process is process:
                self._active_submission_process = None
                self._active_submission_record_id = None
                self._active_submission_started_at = None

    def _reset_odoo_client_after_unsafe_transport(self) -> None:
        reset_session = getattr(self._odoo_client, "reset_session", None)
        close = getattr(self._odoo_client, "close", None)
        if callable(reset_session):
            reset_session()
        elif callable(close):
            close()

    def _record_parent_fake_submission(self, payload: dict[str, str]) -> None:
        submitted_payloads = getattr(self._odoo_client, "submitted_payloads", None)
        if isinstance(submitted_payloads, list):
            submitted_payloads.append(payload)

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


def format_xmlrpc_submission_timeout_log(
    record: MessageRecord,
    elapsed_seconds: float,
    timeout_seconds: int,
    terminated: bool,
    killed: bool,
    exitcode: int | None,
) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Odoo XML-RPC Submission Timed Out",
            f"Database ID       : {record.id}",
            f"Serial            : {record.serial}",
            f"Elapsed Seconds   : {elapsed_seconds:.6f}",
            f"Timeout Threshold : {timeout_seconds}",
            f"Child Terminated  : {terminated}",
            f"Child Killed      : {killed}",
            f"Child Exit Code   : {exitcode}",
            "Manual Verification : required",
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


def _timestamp_optional(value: datetime | None) -> str | None:
    return _timestamp(value) if value is not None else None


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _submit_payload_in_child(
    client_spec: OdooClientSettings | object,
    payload: dict[str, str],
    result_path: Path,
) -> None:
    """Run one Odoo submission in a killable child process."""
    try:
        if isinstance(client_spec, OdooClientSettings):
            client = OdooXmlRpcClient.from_settings(client_spec)
        else:
            client = client_spec

        response = client.submit_print_data(payload)
        _write_child_result(result_path, {"ok": True, "response": response})
    except Exception as exc:
        _write_child_result(
            result_path,
            {
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
    finally:
        close = getattr(locals().get("client", None), "close", None)
        if callable(close):
            close()


def _write_child_result(result_path: Path, result: dict[str, object]) -> None:
    result_path.parent.mkdir(parents=True, exist_ok=True)
    with result_path.open("wb") as handle:
        pickle.dump(result, handle)


def _read_child_result(result_path: Path) -> dict[str, object] | None:
    if not result_path.exists():
        return None
    with result_path.open("rb") as handle:
        result = pickle.load(handle)
    return result if isinstance(result, dict) else None


def _remove_child_result(result_path: Path) -> None:
    try:
        result_path.unlink()
    except FileNotFoundError:
        pass
