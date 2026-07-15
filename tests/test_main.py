from __future__ import annotations

import unittest
from pathlib import Path

from app.config import AppConfig
from app.main import create_odoo_worker


class TestMainLifecycle(unittest.TestCase):
    def test_worker_disabled_when_odoo_enabled_false(self) -> None:
        config = AppConfig(
            mqtt_host="localhost",
            mqtt_port=1883,
            mqtt_client_id="BLACKBEAR_PYTHON_BRIDGE_DEV",
            mqtt_plc_to_odoo_topic="MQTT/PLC_TO_ODOO/topic",
            mqtt_odoo_to_plc_topic="MQTT/ODOO_TO_PLC/topic",
            mqtt_keepalive=60,
            database_path=Path("data/bridge.db"),
            log_level="INFO",
            odoo_enabled=False,
            odoo_url="https://test-bbw.odoo.com",
            odoo_database="broadtechit-test-bbw-stage-34933250",
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

        self.assertIsNone(create_odoo_worker(config))


if __name__ == "__main__":
    unittest.main()
