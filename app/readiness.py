"""BEB-to-PLC readiness monitoring for the Odoo communication path."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


LOGGER = logging.getLogger(__name__)


class ReadinessState(str, Enum):
    UNKNOWN = "UNKNOWN"
    READY = "READY"
    NOT_READY = "NOT_READY"
    DISABLED = "DISABLED"


@dataclass(frozen=True, slots=True)
class ReadinessSnapshot:
    enabled: bool
    state: str
    check_timeout_seconds: float
    last_check_at: str | None
    last_success_at: str | None
    last_failure_at: str | None
    last_published_at: str | None
    last_error: str | None
    disconnect_elapsed_seconds: float | None
    recovery_elapsed_seconds: float | None


class ReadinessRuntimeState:
    """Thread-safe readiness state for monitoring and health reporting."""

    def __init__(self, enabled: bool, check_timeout_seconds: float) -> None:
        self._lock = threading.Lock()
        self._enabled = enabled
        self._check_timeout_seconds = check_timeout_seconds
        self._state = ReadinessState.UNKNOWN if enabled else ReadinessState.DISABLED
        self._last_check_at: datetime | None = None
        self._last_success_at: datetime | None = None
        self._last_failure_at: datetime | None = None
        self._last_published_at: datetime | None = None
        self._last_error: str | None = None
        self._disconnect_started_at: datetime | None = None
        self._recovery_started_at: datetime | None = None

    def record_check(
        self,
        *,
        success: bool,
        checked_at: datetime,
        error: str | None,
    ) -> None:
        with self._lock:
            self._last_check_at = checked_at
            self._last_error = error
            if success:
                self._last_success_at = checked_at
            else:
                self._last_failure_at = checked_at

    def state(self) -> ReadinessState:
        with self._lock:
            return self._state

    def set_state(self, state: ReadinessState) -> None:
        with self._lock:
            self._state = state

    def mark_published(self, published_at: datetime) -> None:
        with self._lock:
            self._last_published_at = published_at

    def disconnect_started_at(self) -> datetime | None:
        with self._lock:
            return self._disconnect_started_at

    def recovery_started_at(self) -> datetime | None:
        with self._lock:
            return self._recovery_started_at

    def set_disconnect_started_at(self, value: datetime | None) -> None:
        with self._lock:
            self._disconnect_started_at = value

    def set_recovery_started_at(self, value: datetime | None) -> None:
        with self._lock:
            self._recovery_started_at = value

    def snapshot(self, now: datetime | None = None) -> ReadinessSnapshot:
        now = now or datetime.now(timezone.utc)
        with self._lock:
            disconnect_elapsed = _elapsed_seconds(self._disconnect_started_at, now)
            recovery_elapsed = _elapsed_seconds(self._recovery_started_at, now)
            return ReadinessSnapshot(
                enabled=self._enabled,
                state=self._state.value,
                check_timeout_seconds=self._check_timeout_seconds,
                last_check_at=_timestamp_optional(self._last_check_at),
                last_success_at=_timestamp_optional(self._last_success_at),
                last_failure_at=_timestamp_optional(self._last_failure_at),
                last_published_at=_timestamp_optional(self._last_published_at),
                last_error=self._last_error,
                disconnect_elapsed_seconds=disconnect_elapsed,
                recovery_elapsed_seconds=recovery_elapsed,
            )


class ReadinessMonitor:
    """Debounce Odoo readiness and publish BR payloads only on confirmed changes."""

    def __init__(
        self,
        *,
        enabled: bool,
        check_interval_seconds: float,
        check_timeout_seconds: float,
        disconnect_delay_seconds: float,
        recovery_delay_seconds: float,
        topic: str,
        readiness_check: Callable[[], bool],
        publisher: Callable[[int], Any],
    ) -> None:
        self._enabled = enabled
        self._check_interval_seconds = check_interval_seconds
        self._check_timeout_seconds = check_timeout_seconds
        self._disconnect_delay_seconds = disconnect_delay_seconds
        self._recovery_delay_seconds = recovery_delay_seconds
        self._topic = topic
        self._readiness_check = readiness_check
        self._publisher = publisher
        self._state = ReadinessRuntimeState(enabled, check_timeout_seconds)
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if not self._enabled:
            LOGGER.info("Readiness monitor disabled")
            return

        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                LOGGER.warning("Readiness monitor already running")
                return

            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="beb-readiness-monitor",
                daemon=True,
            )
            self._thread.start()
            LOGGER.info(
                "Readiness monitor started interval=%s timeout=%s topic=%s",
                self._check_interval_seconds,
                self._check_timeout_seconds,
                self._topic,
            )

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=max(1.0, self._check_interval_seconds * 2))
        LOGGER.info("Readiness monitor stopped")

    def snapshot(self) -> ReadinessSnapshot:
        return self._state.snapshot()

    def run_check_once(self, checked_at: datetime | None = None) -> bool:
        """Run one readiness check and apply debounce rules."""
        if not self._enabled:
            LOGGER.debug("Readiness monitor check skipped because feature is disabled")
            return False

        now = checked_at or datetime.now(timezone.utc)
        success, error = self._run_readiness_check()
        self._state.record_check(success=success, checked_at=now, error=error)
        LOGGER.info(
            "Readiness raw check result=%s state=%s error=%s",
            success,
            self._state.state().value,
            error,
        )

        if success:
            self._handle_success(now)
        else:
            self._handle_failure(now, error)

        return success

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_check_once()
            except Exception:
                LOGGER.exception("Unexpected readiness monitor error")
            self._stop_event.wait(self._check_interval_seconds)

    def _run_readiness_check(self) -> tuple[bool, str | None]:
        try:
            return bool(self._readiness_check()), None
        except Exception as exc:
            LOGGER.warning("Readiness check exception error=%s", exc)
            return False, str(exc)

    def _handle_success(self, now: datetime) -> None:
        current_state = self._state.state()
        if current_state is ReadinessState.READY:
            if self._state.disconnect_started_at() is not None:
                LOGGER.info(
                    "Readiness disconnect debounce reset state=%s raw_check_result=True",
                    current_state.value,
                )
            self._state.set_disconnect_started_at(None)
            self._state.set_recovery_started_at(None)
            return

        if self._state.disconnect_started_at() is not None:
            LOGGER.info(
                "Readiness disconnect debounce reset state=%s raw_check_result=True",
                current_state.value,
            )
        self._state.set_disconnect_started_at(None)

        recovery_started_at = self._state.recovery_started_at()
        if recovery_started_at is None:
            self._state.set_recovery_started_at(now)
            LOGGER.info(
                "Readiness recovery debounce started state=%s raw_check_result=True",
                current_state.value,
            )
            return

        elapsed = (now - recovery_started_at).total_seconds()
        if elapsed >= self._recovery_delay_seconds:
            self._confirm_state(ReadinessState.READY, 1, now, elapsed, "recovery")

    def _handle_failure(self, now: datetime, error: str | None) -> None:
        current_state = self._state.state()
        if self._state.recovery_started_at() is not None:
            LOGGER.info(
                "Readiness recovery debounce reset state=%s raw_check_result=False error=%s",
                current_state.value,
                error,
            )
        self._state.set_recovery_started_at(None)

        if current_state is ReadinessState.UNKNOWN:
            self._confirm_state(ReadinessState.NOT_READY, 0, now, 0.0, "startup")
            return

        if current_state is ReadinessState.NOT_READY:
            self._state.set_disconnect_started_at(None)
            return

        disconnect_started_at = self._state.disconnect_started_at()
        if disconnect_started_at is None:
            self._state.set_disconnect_started_at(now)
            LOGGER.info(
                "Readiness disconnect debounce started state=%s raw_check_result=False error=%s",
                current_state.value,
                error,
            )
            return

        elapsed = (now - disconnect_started_at).total_seconds()
        if elapsed >= self._disconnect_delay_seconds:
            self._confirm_state(ReadinessState.NOT_READY, 0, now, elapsed, "disconnect")

    def _confirm_state(
        self,
        new_state: ReadinessState,
        br_value: int,
        now: datetime,
        elapsed_seconds: float,
        reason: str,
    ) -> None:
        previous_state = self._state.state()
        if previous_state is new_state:
            return

        self._state.set_state(new_state)
        self._state.set_disconnect_started_at(None)
        self._state.set_recovery_started_at(None)
        LOGGER.info(
            "Readiness confirmed state changed previous=%s new=%s raw_check_result=%s elapsed=%.3f reason=%s",
            previous_state.value,
            new_state.value,
            br_value == 1,
            elapsed_seconds,
            reason,
        )
        self._publish_br(br_value, now)

    def _publish_br(self, br_value: int, now: datetime) -> None:
        payload = {"BR": br_value}
        try:
            result = self._publisher(br_value)
        except Exception as exc:
            LOGGER.error(
                "BR publish failure topic=%s payload=%s error=%s",
                self._topic,
                payload,
                exc,
            )
            return

        success = bool(result) if not hasattr(result, "success") else bool(result.success)
        if success:
            self._state.mark_published(now)
            LOGGER.info("BR payload published topic=%s payload=%s", self._topic, payload)
            return

        error = getattr(result, "error", "publish returned false")
        LOGGER.error("BR publish failure topic=%s payload=%s error=%s", self._topic, payload, error)


def _timestamp_optional(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _elapsed_seconds(started_at: datetime | None, now: datetime) -> float | None:
    if started_at is None:
        return None
    return max(0.0, (now - started_at).total_seconds())
