from __future__ import annotations

import unittest
from datetime import datetime, timezone

from app.message_parser import ParsedPLCMessage
from app.mqtt_client import format_received_message_log


class TestMqttLogging(unittest.TestCase):
    def test_formatted_receive_log_contains_required_fields(self) -> None:
        parsed = ParsedPLCMessage(
            message_type="MN",
            model_number="106-020C012P001",
            serial_number="3241",
            table_number="T01",
        )

        log_text = format_received_message_log(
            topic="MQTT/PLC_TO_ODOO/topic",
            raw_payload='{"MN":"106-020C012P0013241T01"}',
            parsed=parsed,
            timestamp=datetime(2026, 7, 15, 10, 0, 0, tzinfo=timezone.utc),
        )

        self.assertIn("Received MQTT Message", log_text)
        self.assertIn("Timestamp  : 2026-07-15T10:00:00+00:00", log_text)
        self.assertIn("Topic      : MQTT/PLC_TO_ODOO/topic", log_text)
        self.assertIn('Raw Payload: {"MN":"106-020C012P0013241T01"}', log_text)
        self.assertIn("Type       : MN (Print Completed)", log_text)
        self.assertIn("Table      : T01", log_text)
        self.assertIn("Model      : 106-020C012P001", log_text)
        self.assertIn("Serial     : 3241", log_text)


if __name__ == "__main__":
    unittest.main()
