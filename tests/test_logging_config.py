from __future__ import annotations

import logging
import unittest
from datetime import datetime, timezone

from app.logging_config import ISTFormatter


class TestISTFormatter(unittest.TestCase):
    def test_formats_application_log_timestamp_in_ist_with_milliseconds(self) -> None:
        record = logging.LogRecord(
            name="app.readiness",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="Readiness confirmed state changed previous=READY new=NOT_READY",
            args=(),
            exc_info=None,
        )
        timestamp = datetime(
            2026,
            7,
            23,
            10,
            51,
            15,
            538000,
            tzinfo=timezone.utc,
        )
        record.created = timestamp.timestamp()
        record.msecs = 538

        output = ISTFormatter().format(record)

        self.assertEqual(
            output,
            "[23-Jul-2026 04:21:15.538 PM IST] [READINESS] "
            "Readiness confirmed state changed previous=READY new=NOT_READY",
        )

    def test_unknown_app_logger_gets_stable_component_name(self) -> None:
        record = logging.LogRecord(
            name="app.example_worker",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        record.created = datetime(2026, 7, 23, tzinfo=timezone.utc).timestamp()
        record.msecs = 0

        output = ISTFormatter().format(record)

        self.assertIn("[EXAMPLE_WORKER] hello", output)


if __name__ == "__main__":
    unittest.main()
