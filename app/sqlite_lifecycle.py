"""SQLite reconciliation and raw-retention cleanup scheduling."""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.database import (
    CleanupResult,
    cleanup_raw_operational_data,
    reconcile_completed_production_records,
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SQLiteLifecycleSnapshot:
    last_cleanup_at: str | None
    last_cleanup_deleted_rows: int | None
    last_cleanup_error: str | None


class SQLiteLifecycleManager:
    """Run bounded reconciliation and raw cleanup without production side effects."""

    def __init__(
        self,
        *,
        database_path: str | Path,
        machine_id: str,
        retention_days: int,
        cleanup_enabled: bool,
        cleanup_interval_hours: int,
        cleanup_batch_size: int,
        vacuum_enabled: bool,
        reconcile_batch_size: int,
    ) -> None:
        self._database_path = Path(database_path)
        self._machine_id = machine_id
        self._retention_days = retention_days
        self._cleanup_enabled = cleanup_enabled
        self._cleanup_interval_seconds = max(1, cleanup_interval_hours) * 3600
        self._cleanup_batch_size = cleanup_batch_size
        self._vacuum_enabled = vacuum_enabled
        self._reconcile_batch_size = reconcile_batch_size
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._last_cleanup_at: str | None = None
        self._last_cleanup_deleted_rows: int | None = None
        self._last_cleanup_error: str | None = None

    def start(self) -> None:
        self.reconcile_once()
        if not self._cleanup_enabled:
            LOGGER.info("SQLite raw cleanup disabled")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._cleanup_loop,
            name="sqlite-lifecycle-cleanup",
            daemon=True,
        )
        self._thread.start()
        LOGGER.info(
            "SQLite raw cleanup started retention_days=%s interval_hours=%.3f batch_size=%s vacuum_enabled=%s",
            self._retention_days,
            self._cleanup_interval_seconds / 3600,
            self._cleanup_batch_size,
            self._vacuum_enabled,
        )

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)
        LOGGER.info("SQLite lifecycle manager stopped")

    def reconcile_once(self) -> None:
        reconcile_completed_production_records(
            self._database_path,
            machine_id=self._machine_id,
            limit=self._reconcile_batch_size,
        )

    def cleanup_once(self) -> CleanupResult:
        result = cleanup_raw_operational_data(
            self._database_path,
            retention_days=self._retention_days,
            batch_size=self._cleanup_batch_size,
            vacuum_enabled=self._vacuum_enabled,
        )
        self._record_cleanup_result(result)
        return result

    def snapshot(self) -> SQLiteLifecycleSnapshot:
        with self._lock:
            return SQLiteLifecycleSnapshot(
                last_cleanup_at=self._last_cleanup_at,
                last_cleanup_deleted_rows=self._last_cleanup_deleted_rows,
                last_cleanup_error=self._last_cleanup_error,
            )

    def _cleanup_loop(self) -> None:
        while not self._stop_event.is_set():
            self.cleanup_once()
            self._stop_event.wait(self._cleanup_interval_seconds)

    def _record_cleanup_result(self, result: CleanupResult) -> None:
        with self._lock:
            self._last_cleanup_at = datetime.now(timezone.utc).isoformat(
                timespec="seconds"
            )
            self._last_cleanup_deleted_rows = result.total_deleted_rows
            self._last_cleanup_error = result.error
