from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import paho.mqtt.client as mqtt

from app.mqtt_client import publish_ack
from app.queue_worker import extract_ack


class TestAckPublishing(unittest.TestCase):
    def test_ack_extracted_correctly(self) -> None:
        response = {
            "success": True,
            "result": {
                "ACK": "3243",
            },
        }

        self.assertEqual(extract_ack(response), "3243")

    def test_missing_ack_returns_none(self) -> None:
        response = {
            "success": True,
            "result": {},
        }

        self.assertIsNone(extract_ack(response))

    def test_success_false_returns_none(self) -> None:
        response = {
            "success": False,
            "result": {
                "ACK": "3243",
            },
        }

        self.assertIsNone(extract_ack(response))

    def test_mqtt_publish_called_once_with_correct_topic_and_payload(self) -> None:
        client = MagicMock()
        client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS)

        result = publish_ack(client, "MQTT/ODOO_TO_PLC/topic", "3243")

        self.assertTrue(result)
        client.publish.assert_called_once_with(
            "MQTT/ODOO_TO_PLC/topic",
            payload='{"ACK":"3243"}',
            qos=0,
            retain=False,
        )

    def test_publish_failure_returns_false(self) -> None:
        client = MagicMock()
        client.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_NO_CONN)

        result = publish_ack(client, "MQTT/ODOO_TO_PLC/topic", "3243")

        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()
