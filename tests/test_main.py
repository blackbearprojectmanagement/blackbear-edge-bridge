from __future__ import annotations

import unittest
from pathlib import Path

from app.config import AppConfig
from app.main import create_odoo_worker, create_readiness_monitor


class TestMainLifecycle(unittest.TestCase):
    def make_config(self, **overrides: object) -> AppConfig:
        values = {
            "mqtt_host": "localhost",
            "mqtt_port": 1883,
            "mqtt_client_id": "BLACKBEAR_PYTHON_BRIDGE_DEV",
            "mqtt_plc_to_odoo_topic": "MQTT/PLC_TO_ODOO/topic",
            "mqtt_odoo_to_plc_topic": "MQTT/ODOO_TO_PLC/topic",
            "mqtt_keepalive": 60,
            "database_path": Path("data/bridge.db"),
            "log_level": "INFO",
            "odoo_enabled": False,
            "odoo_url": "https://test-bbw.odoo.com",
            "odoo_database": "broadtechit-test-bbw-stage-34933250",
            "odoo_username": "admin",
            "odoo_password": "secret",
            "odoo_model": "iot.configuration",
            "odoo_submit_method": "xmlrpc_submit_print_data",
            "odoo_timeout": 15,
            "odoo_worker_interval": 2,
            "odoo_batch_size": 10,
            "odoo_max_retries": 10,
            "odoo_stale_processing_seconds": 300,
        }
        values.update(overrides)
        return AppConfig(**values)

    def test_worker_disabled_when_odoo_enabled_false(self) -> None:
        config = self.make_config(odoo_enabled=False)

        self.assertIsNone(create_odoo_worker(config))

    def test_readiness_monitor_disabled_when_odoo_disabled(self) -> None:
        config = self.make_config(odoo_enabled=False, beb_ready_enabled=True)

        class FakeMqtt:
            def publish_readiness(self, value: int) -> bool:
                raise AssertionError("disabled monitor should not publish")

        monitor, odoo_client = create_readiness_monitor(config, FakeMqtt())

        self.assertIsNone(odoo_client)
        self.assertFalse(monitor.snapshot().enabled)
        self.assertEqual(monitor.snapshot().state, "DISABLED")

    def test_readiness_client_uses_short_timeout_independent_of_odoo_timeout(self) -> None:
        config = self.make_config(
            odoo_enabled=True,
            odoo_timeout=90,
            beb_ready_enabled=True,
            beb_ready_check_timeout_seconds=3,
        )

        class FakeMqtt:
            def publish_readiness(self, value: int) -> bool:
                return True

        worker_bundle = create_odoo_worker(config)
        self.assertIsNotNone(worker_bundle)
        worker, worker_odoo_client = worker_bundle
        readiness_monitor, readiness_odoo_client = create_readiness_monitor(
            config,
            FakeMqtt(),
        )

        self.assertEqual(worker._submission_timeout, 90)
        self.assertEqual(worker_odoo_client.timeout, 90)
        self.assertIsNotNone(readiness_odoo_client)
        self.assertEqual(readiness_odoo_client.timeout, 3)
        self.assertEqual(readiness_monitor.snapshot().check_timeout_seconds, 3)


if __name__ == "__main__":
    unittest.main()
