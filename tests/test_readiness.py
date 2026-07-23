from __future__ import annotations

import unittest
import logging
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.database import initialize_api_commands_table, initialize_database
from app.readiness import ReadinessMonitor


BASE_TIME = datetime(2026, 7, 21, 10, 0, 0, tzinfo=timezone.utc)


class ScriptedCheck:
    def __init__(self, results: list[bool | Exception]) -> None:
        self.results = list(results)
        self.calls = 0

    def __call__(self) -> bool:
        self.calls += 1
        if not self.results:
            return False
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class RecordingPublisher:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.values: list[int] = []

    def __call__(self, value: int) -> bool:
        self.values.append(value)
        if self.fail:
            raise RuntimeError("mqtt unavailable")
        return True


def at(seconds: float) -> datetime:
    return BASE_TIME + timedelta(seconds=seconds)


def make_monitor(
    check: ScriptedCheck,
    publisher: RecordingPublisher,
    *,
    enabled: bool = True,
) -> ReadinessMonitor:
    return ReadinessMonitor(
        enabled=enabled,
        check_interval_seconds=1,
        check_timeout_seconds=3,
        disconnect_delay_seconds=5,
        recovery_delay_seconds=10,
        topic="MQTT/ODOO_TO_PLC/topic",
        readiness_check=check,
        publisher=publisher,
    )


class TestReadinessMonitor(unittest.TestCase):
    def test_startup_requires_continuous_success_before_ready_publish(self) -> None:
        check = ScriptedCheck([True, True, True])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)

        monitor.run_check_once(at(0))
        monitor.run_check_once(at(9.9))
        monitor.run_check_once(at(10))

        self.assertEqual(publisher.values, [1])
        self.assertEqual(monitor.snapshot().state, "READY")

    def test_initial_failure_publishes_not_ready_once(self) -> None:
        check = ScriptedCheck([False, False, False])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)

        monitor.run_check_once(at(0))
        monitor.run_check_once(at(1))
        monitor.run_check_once(at(2))

        self.assertEqual(publisher.values, [0])
        self.assertEqual(monitor.snapshot().state, "NOT_READY")

    def test_ready_disconnect_uses_five_second_debounce(self) -> None:
        check = ScriptedCheck([True, True, False, False, False])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)

        monitor.run_check_once(at(0))
        monitor.run_check_once(at(10))
        monitor.run_check_once(at(11))
        monitor.run_check_once(at(15.9))
        monitor.run_check_once(at(16))

        self.assertEqual(publisher.values, [1, 0])
        self.assertEqual(monitor.snapshot().state, "NOT_READY")

    def test_stable_ready_does_not_republish(self) -> None:
        check = ScriptedCheck([True, True, True, True])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)

        monitor.run_check_once(at(0))
        monitor.run_check_once(at(10))
        monitor.run_check_once(at(11))
        monitor.run_check_once(at(12))

        self.assertEqual(publisher.values, [1])
        self.assertEqual(monitor.snapshot().state, "READY")

    def test_stable_ready_success_checks_do_not_emit_info_logs(self) -> None:
        check = ScriptedCheck([True, True, True, True])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)
        monitor.run_check_once(at(0))
        monitor.run_check_once(at(10))

        handler = _RecordingLogHandler()
        logger = logging.getLogger("app.readiness")
        original_level = logger.level
        logger.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        try:
            monitor.run_check_once(at(11))
            monitor.run_check_once(at(12))
        finally:
            logger.removeHandler(handler)
            logger.setLevel(original_level)

        info_messages = [
            record.getMessage()
            for record in handler.records
            if record.levelno >= logging.INFO
        ]
        self.assertEqual(info_messages, [])

    def test_confirmed_transition_still_emits_info_logs(self) -> None:
        check = ScriptedCheck([False])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)

        with self.assertLogs("app.readiness", level="INFO") as logs:
            monitor.run_check_once(at(0))

        log_text = "\n".join(logs.output)
        self.assertIn("Readiness confirmed state changed", log_text)
        self.assertIn("previous=UNKNOWN new=NOT_READY", log_text)
        self.assertIn("BR payload published", log_text)

    def test_brief_disconnect_is_suppressed_and_timer_resets(self) -> None:
        check = ScriptedCheck([True, True, False, True, False, False])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)

        monitor.run_check_once(at(0))
        monitor.run_check_once(at(10))
        monitor.run_check_once(at(11))
        monitor.run_check_once(at(12))
        monitor.run_check_once(at(13))
        monitor.run_check_once(at(17.9))

        self.assertEqual(publisher.values, [1])
        self.assertEqual(monitor.snapshot().state, "READY")

    def test_not_ready_recovery_uses_ten_second_debounce(self) -> None:
        check = ScriptedCheck([False, True, True, True])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)

        monitor.run_check_once(at(0))
        monitor.run_check_once(at(1))
        monitor.run_check_once(at(10.9))
        monitor.run_check_once(at(11))

        self.assertEqual(publisher.values, [0, 1])
        self.assertEqual(monitor.snapshot().state, "READY")

    def test_brief_recovery_is_suppressed_and_timer_resets(self) -> None:
        check = ScriptedCheck([False, True, False, True, True])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)

        monitor.run_check_once(at(0))
        monitor.run_check_once(at(1))
        monitor.run_check_once(at(2))
        monitor.run_check_once(at(3))
        monitor.run_check_once(at(12.9))

        self.assertEqual(publisher.values, [0])
        self.assertEqual(monitor.snapshot().state, "NOT_READY")

    def test_publish_exception_does_not_crash_monitor(self) -> None:
        check = ScriptedCheck([False])
        publisher = RecordingPublisher(fail=True)
        monitor = make_monitor(check, publisher)

        monitor.run_check_once(at(0))

        self.assertEqual(publisher.values, [0])
        self.assertEqual(monitor.snapshot().state, "NOT_READY")
        self.assertIsNone(monitor.snapshot().last_published_at)

    def test_odoo_check_exception_is_treated_as_failure(self) -> None:
        check = ScriptedCheck([RuntimeError("odoo unavailable")])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher)

        result = monitor.run_check_once(at(0))
        snapshot = monitor.snapshot()

        self.assertFalse(result)
        self.assertEqual(publisher.values, [0])
        self.assertEqual(snapshot.state, "NOT_READY")
        self.assertEqual(snapshot.last_error, "odoo unavailable")

    def test_disabled_monitor_does_not_check_or_publish(self) -> None:
        check = ScriptedCheck([True])
        publisher = RecordingPublisher()
        monitor = make_monitor(check, publisher, enabled=False)

        result = monitor.run_check_once(at(0))
        snapshot = monitor.snapshot()

        self.assertFalse(result)
        self.assertEqual(check.calls, 0)
        self.assertEqual(publisher.values, [])
        self.assertFalse(snapshot.enabled)
        self.assertEqual(snapshot.state, "DISABLED")
        self.assertEqual(snapshot.check_timeout_seconds, 3)

    def test_readiness_checks_do_not_insert_transaction_rows(self) -> None:
        temp_root = Path.cwd() / "data"
        temp_root.mkdir(parents=True, exist_ok=True)
        database_path = temp_root / f"test_readiness_{uuid.uuid4().hex}.db"
        try:
            initialize_database(database_path)
            initialize_api_commands_table(database_path)
            check = ScriptedCheck([True, True])
            publisher = RecordingPublisher()
            monitor = make_monitor(check, publisher)

            monitor.run_check_once(at(0))
            monitor.run_check_once(at(10))

            with closing(sqlite3.connect(database_path)) as connection:
                mqtt_count = connection.execute(
                    "SELECT COUNT(*) FROM mqtt_messages"
                ).fetchone()[0]
                api_count = connection.execute(
                    "SELECT COUNT(*) FROM api_commands"
                ).fetchone()[0]

            self.assertEqual(mqtt_count, 0)
            self.assertEqual(api_count, 0)
        finally:
            for path in (
                database_path,
                database_path.with_name(f"{database_path.name}-wal"),
                database_path.with_name(f"{database_path.name}-shm"),
            ):
                if path.exists():
                    path.unlink()


class _RecordingLogHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


if __name__ == "__main__":
    unittest.main()
