from __future__ import annotations

import json
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import paho.mqtt.client as mqtt

from app.config import AppConfig
from app.database import (
    get_message_by_id,
    mark_completed,
    mark_failed,
    record_ack_replay_attempt,
    save_message,
)
from app.mqtt_client import BEBMqttClient


TOPIC = "MQTT/PLC_TO_ODOO/topic"
ODOO_TOPIC = "MQTT/ODOO_TO_PLC/topic"


class TestDuplicateAckReplay(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / "data"
        temp_root.mkdir(parents=True, exist_ok=True)
        self.database_path = temp_root / f"test_mqtt_duplicate_{uuid.uuid4().hex}.db"

    def tearDown(self) -> None:
        for path in (
            self.database_path,
            self.database_path.with_name(f"{self.database_path.name}-wal"),
            self.database_path.with_name(f"{self.database_path.name}-shm"),
        ):
            if path.exists():
                path.unlink()

    def test_duplicate_successful_message_replays_stored_ack_only(self) -> None:
        raw_payload = '{"MN":"106-020C012P0013272T01"}'
        saved = self._save(raw_payload, serial="3272")
        response = {
            "success": True,
            "result": {
                "ACK": "3272",
                "customer_id": 145,
                "customer_name": "Mahindra",
            },
        }
        mark_completed(
            saved.id,
            json.dumps(response),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            ack="3272",
            customer_id=145,
            customer_name="Mahindra",
        )
        bridge, fake_paho, handler = self._client()

        with self.assertLogs("app.mqtt_client", level="INFO") as logs:
            bridge._on_message(
                fake_paho,
                None,
                SimpleNamespace(topic=TOPIC, payload=raw_payload.encode("utf-8")),
            )

        handler.assert_not_called()
        fake_paho.publish.assert_called_once_with(
            ODOO_TOPIC,
            payload='{"ACK": "3272"}',
            qos=0,
            retain=False,
        )
        record = get_message_by_id(saved.id, self.database_path)
        self.assertEqual(record.ack_replay_count, 1)
        self.assertIn("Replaying stored ACK for duplicate message", "\n".join(logs.output))

    def test_duplicate_failed_message_does_not_replay_ack(self) -> None:
        raw_payload = '{"MN":"106-020C012P0013273T01"}'
        saved = self._save(raw_payload, serial="3273")
        mark_failed(
            saved.id,
            "No printer configured",
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            odoo_response=json.dumps(
                {"success": False, "result": {"ACK": "3273"}}
            ),
        )
        bridge, fake_paho, handler = self._client()

        with self.assertLogs("app.mqtt_client", level="INFO") as logs:
            bridge._on_message(
                fake_paho,
                None,
                SimpleNamespace(topic=TOPIC, payload=raw_payload.encode("utf-8")),
            )

        handler.assert_not_called()
        fake_paho.publish.assert_not_called()
        self.assertIn("Duplicate ACK replay suppressed", "\n".join(logs.output))

    def test_duplicate_completed_record_without_ack_does_not_replay(self) -> None:
        raw_payload = '{"MN":"106-020C012P0013274T01"}'
        saved = self._save(raw_payload, serial="3274")
        mark_completed(
            saved.id,
            json.dumps({"success": True, "result": {}}),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
        )
        bridge, fake_paho, _handler = self._client()

        with self.assertLogs("app.mqtt_client", level="WARNING") as logs:
            bridge._on_message(
                fake_paho,
                None,
                SimpleNamespace(topic=TOPIC, payload=raw_payload.encode("utf-8")),
            )

        fake_paho.publish.assert_not_called()
        self.assertIn("missing ACK", "\n".join(logs.output))

    def test_duplicate_replay_minimum_interval_and_limit(self) -> None:
        raw_payload = '{"MN":"106-020C012P0013275T01"}'
        saved = self._save(raw_payload, serial="3275")
        mark_completed(
            saved.id,
            json.dumps({"success": True, "result": {"ACK": "3275"}}),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            ack="3275",
        )
        bridge, fake_paho, _handler = self._client()
        message = SimpleNamespace(topic=TOPIC, payload=raw_payload.encode("utf-8"))

        bridge._on_message(fake_paho, None, message)
        with self.assertLogs("app.mqtt_client", level="INFO") as interval_logs:
            bridge._on_message(fake_paho, None, message)

        self.assertEqual(fake_paho.publish.call_count, 1)
        self.assertIn("minimum interval", "\n".join(interval_logs.output))

        limited_payload = '{"MN":"106-020C012P0013276T01"}'
        limited = self._save(limited_payload, serial="3276")
        mark_completed(
            limited.id,
            json.dumps({"success": True, "result": {"ACK": "3276"}}),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            ack="3276",
        )
        base = datetime.now(timezone.utc) - timedelta(seconds=10)
        for offset in (0, 2, 4):
            record_ack_replay_attempt(
                limited.message_hash,
                self.database_path,
                base + timedelta(seconds=offset),
            )

        with self.assertLogs("app.mqtt_client", level="WARNING") as limit_logs:
            bridge._on_message(
                fake_paho,
                None,
                SimpleNamespace(topic=TOPIC, payload=limited_payload.encode("utf-8")),
            )

        self.assertEqual(fake_paho.publish.call_count, 1)
        self.assertIn("replay limit reached", "\n".join(limit_logs.output).lower())

    def test_duplicate_legacy_repr_response_parsing(self) -> None:
        raw_payload = '{"MN":"106-020C012P0013277T01"}'
        saved = self._save(raw_payload, serial="3277")
        mark_completed(
            saved.id,
            repr({"success": True, "result": {"ACK": "3277"}}),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            ack="3277",
        )
        bridge, fake_paho, _handler = self._client()

        bridge._on_message(
            fake_paho,
            None,
            SimpleNamespace(topic=TOPIC, payload=raw_payload.encode("utf-8")),
        )

        fake_paho.publish.assert_called_once_with(
            ODOO_TOPIC,
            payload='{"ACK": "3277"}',
            qos=0,
            retain=False,
        )

    def _client(self):
        config = AppConfig(
            mqtt_host="localhost",
            mqtt_port=1883,
            mqtt_client_id="BLACKBEAR_PYTHON_BRIDGE_DEV",
            mqtt_plc_to_odoo_topic=TOPIC,
            mqtt_odoo_to_plc_topic=ODOO_TOPIC,
            mqtt_keepalive=60,
            database_path=self.database_path,
            log_level="INFO",
            odoo_enabled=True,
            odoo_url="https://test-bbw.odoo.com",
            odoo_database="broadtechit-test-bbbw-stage-34933250",
            odoo_username="admin",
            odoo_password="secret",
            odoo_model="iot.configuration",
            odoo_submit_method="xmlrpc_submit_print_data",
            odoo_timeout=15,
            odoo_worker_interval=2,
            odoo_batch_size=10,
            odoo_max_retries=10,
            odoo_stale_processing_seconds=300,
        )
        fake_paho = MagicMock()
        fake_paho.publish.return_value = SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS)
        handler = MagicMock()
        bridge = BEBMqttClient.__new__(BEBMqttClient)
        bridge._config = config
        bridge._client = fake_paho
        bridge._message_handler = handler
        return bridge, fake_paho, handler

    def _save(self, raw_payload: str, *, serial: str):
        return save_message(
            topic=TOPIC,
            raw_payload=raw_payload,
            message_type="MN",
            table_no="T01",
            model="106-020C012P001",
            serial=serial,
            database_path=self.database_path,
        )


if __name__ == "__main__":
    unittest.main()
