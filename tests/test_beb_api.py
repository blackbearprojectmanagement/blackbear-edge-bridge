from __future__ import annotations

import json
import sqlite3
import time
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import paho.mqtt.client as mqtt
from fastapi.testclient import TestClient

from app.api import create_api_app
from app.api_server import BebApiServer
from app.command_parser import CommandValidationError, validate_plc_command
from app.config import AppConfig
from app.database import (
    claim_pending_messages,
    get_api_command_by_idempotency_key,
    initialize_api_commands_table,
    mark_completed,
    save_message,
    update_status,
)
from app.mqtt_client import BEBMqttClient, PLC_JSON_SEPARATORS, PublishResult
from app.readiness import ReadinessSnapshot


class FakeMqttClient:
    def __init__(
        self,
        *,
        connected: bool = True,
        success: bool = True,
        error: str | None = None,
    ) -> None:
        self.connected = connected
        self.success = success
        self.error = error
        self.calls: list[dict[str, str]] = []

    def is_connected(self) -> bool:
        return self.connected

    def publish_plc_command(self, payload: dict[str, str]) -> PublishResult:
        self.calls.append(payload)
        plc_payload = json.dumps(payload, separators=PLC_JSON_SEPARATORS)
        if not self.success:
            return PublishResult(
                success=False,
                rc=mqtt.MQTT_ERR_NO_CONN,
                mid=None,
                topic="MQTT/ODOO_TO_PLC/topic",
                payload=plc_payload,
                error=self.error or "MQTT broker unavailable",
            )
        return PublishResult(
            success=True,
            rc=mqtt.MQTT_ERR_SUCCESS,
            mid=12,
            topic="MQTT/ODOO_TO_PLC/topic",
            payload=plc_payload,
        )


class FakeWorker:
    def __init__(self, healthy: bool) -> None:
        self.healthy = healthy

    def health_snapshot(self, heartbeat_threshold_seconds: int) -> dict[str, object]:
        return {
            "worker_running": True,
            "worker_healthy": self.healthy,
            "worker_last_heartbeat": "2026-07-21T10:00:00+00:00",
            "worker_current_message_id": None,
            "worker_current_processing_seconds": None,
            "queue_new_count": 1,
            "queue_processing_count": 0,
            "queue_failed_count": 0,
            "queue_completed_count": 1,
        }


class FakeReadinessMonitor:
    def __init__(self, state: str, *, enabled: bool = True) -> None:
        self.state = state
        self.enabled = enabled

    def snapshot(self) -> ReadinessSnapshot:
        return ReadinessSnapshot(
            enabled=self.enabled,
            state=self.state,
            check_timeout_seconds=3,
            last_check_at="2026-07-21T10:00:01+00:00",
            last_success_at="2026-07-21T10:00:01+00:00"
            if self.state == "READY"
            else None,
            last_failure_at=None if self.state == "READY" else "2026-07-21T10:00:01+00:00",
            last_published_at="2026-07-21T10:00:02+00:00",
            last_error=None if self.state == "READY" else "Odoo unavailable",
            disconnect_elapsed_seconds=None,
            recovery_elapsed_seconds=None,
        )


def make_config(database_path: Path, **overrides: object) -> AppConfig:
    values = {
        "mqtt_host": "localhost",
        "mqtt_port": 1883,
        "mqtt_client_id": "BLACKBEAR_PYTHON_BRIDGE_DEV",
        "mqtt_plc_to_odoo_topic": "MQTT/PLC_TO_ODOO/topic",
        "mqtt_odoo_to_plc_topic": "MQTT/ODOO_TO_PLC/topic",
        "mqtt_keepalive": 60,
        "database_path": database_path,
        "log_level": "INFO",
        "odoo_enabled": True,
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
        "beb_api_enabled": True,
        "beb_api_host": "127.0.0.1",
        "beb_api_port": 8000,
        "beb_api_username": "odoo",
        "beb_api_password": "password",
        "beb_api_request_timeout": 10,
        "beb_api_idempotency_ttl_seconds": 86400,
        "beb_api_max_body_bytes": 16384,
        "beb_api_log_request_body": True,
        "beb_ready_enabled": False,
    }
    values.update(overrides)
    return AppConfig(**values)


class ApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / "data"
        temp_root.mkdir(parents=True, exist_ok=True)
        self.database_path = temp_root / f"test_api_{uuid.uuid4().hex}.db"

    def tearDown(self) -> None:
        for path in (
            self.database_path,
            self.database_path.with_name(f"{self.database_path.name}-wal"),
            self.database_path.with_name(f"{self.database_path.name}-shm"),
        ):
            if path.exists():
                path.unlink()

    def make_client(
        self,
        mqtt_client: FakeMqttClient | None = None,
        odoo_worker: object | None = None,
        readiness_monitor: object | None = None,
        **config_overrides: object,
    ) -> tuple[TestClient, FakeMqttClient]:
        config = make_config(self.database_path, **config_overrides)
        fake_mqtt = mqtt_client or FakeMqttClient()
        return (
            TestClient(
                create_api_app(
                    config,
                    fake_mqtt,
                    self.database_path,
                    odoo_worker,
                    readiness_monitor,
                )
            ),
            fake_mqtt,
        )

    def post_command(
        self,
        client: TestClient,
        payload: object,
        *,
        idempotency_key: str | None = "PRINTJOB-2960-T01",
        username: str = "odoo",
        password: str = "password",
    ):
        headers = {}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        return client.post(
            "/api/v1/plc/command",
            json=payload,
            headers=headers,
            auth=(username, password),
        )


class TestBebApi(ApiTestCase):
    def test_health_endpoint(self) -> None:
        client, _ = self.make_client()

        response = client.get("/health")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["service"], "BlackBear Edge Bridge")
        self.assertTrue(response.json()["mqtt_connected"])
        self.assertTrue(response.json()["odoo_enabled"])
        self.assertTrue(response.json()["api_enabled"])
        self.assertIn("worker_running", response.json())
        self.assertIn("worker_healthy", response.json())
        self.assertIn("queue_new_count", response.json())
        self.assertIn("queue_processing_count", response.json())
        self.assertIn("queue_failed_count", response.json())
        self.assertIn("queue_completed_count", response.json())
        self.assertIn("beb_ready_enabled", response.json())
        self.assertIn("beb_ready_state", response.json())
        self.assertIn("beb_ready_check_timeout_seconds", response.json())
        self.assertIn("sqlite_database_size_bytes", response.json())
        self.assertIn("production_records_count", response.json())
        self.assertIn("daily_summary_count", response.json())
        self.assertIn("retention_days", response.json())

    def test_health_degrades_when_worker_unhealthy(self) -> None:
        client, _ = self.make_client(odoo_worker=FakeWorker(healthy=False))

        response = client.get("/health")
        body = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["status"], "degraded")
        self.assertTrue(body["worker_running"])
        self.assertFalse(body["worker_healthy"])
        self.assertEqual(body["worker_last_heartbeat"], "2026-07-21T10:00:00+00:00")

    def test_health_returns_ok_when_worker_recovers(self) -> None:
        client, _ = self.make_client(odoo_worker=FakeWorker(healthy=True))

        response = client.get("/health")
        body = response.json()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(body["status"], "ok")
        self.assertTrue(body["worker_healthy"])
        self.assertEqual(body["queue_new_count"], 1)
        self.assertEqual(body["queue_completed_count"], 1)

    def test_health_reports_readiness_ready_state(self) -> None:
        client, _ = self.make_client(
            odoo_worker=FakeWorker(healthy=True),
            readiness_monitor=FakeReadinessMonitor("READY"),
            beb_ready_enabled=True,
        )

        body = client.get("/health").json()

        self.assertEqual(body["status"], "ok")
        self.assertTrue(body["beb_ready_enabled"])
        self.assertEqual(body["beb_ready_state"], "READY")
        self.assertEqual(body["beb_ready_check_timeout_seconds"], 3)
        self.assertEqual(body["beb_ready_last_success_at"], "2026-07-21T10:00:01+00:00")

    def test_health_degrades_when_readiness_not_ready(self) -> None:
        client, _ = self.make_client(
            odoo_worker=FakeWorker(healthy=True),
            readiness_monitor=FakeReadinessMonitor("NOT_READY"),
            beb_ready_enabled=True,
        )

        body = client.get("/health").json()

        self.assertEqual(body["status"], "degraded")
        self.assertEqual(body["beb_ready_state"], "NOT_READY")
        self.assertEqual(body["beb_ready_last_error"], "Odoo unavailable")

    def test_health_reports_database_queue_counts_without_worker(self) -> None:
        saved_new = save_message(
            topic="MQTT/PLC_TO_ODOO/topic",
            raw_payload='{"MN":"106-020C012P0013001T01"}',
            message_type="MN",
            table_no="T01",
            model="106-020C012P001",
            serial="3001",
            database_path=self.database_path,
        )
        saved_completed = save_message(
            topic="MQTT/PLC_TO_ODOO/topic",
            raw_payload='{"MN":"106-020C012P0013002T01"}',
            message_type="MN",
            table_no="T01",
            model="106-020C012P001",
            serial="3002",
            database_path=self.database_path,
        )
        update_status(saved_completed.id, "COMPLETED", self.database_path)
        client, _ = self.make_client(odoo_enabled=False)

        body = client.get("/health").json()

        self.assertEqual(body["status"], "ok")
        self.assertFalse(body["worker_running"])
        self.assertTrue(body["worker_healthy"])
        self.assertEqual(body["queue_new_count"], 1)
        self.assertEqual(body["queue_completed_count"], 1)

    def test_missing_authentication_returns_401(self) -> None:
        client, _ = self.make_client()

        response = client.post("/api/v1/plc/command", json={"messt01": "Z"})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.headers["www-authenticate"], "Basic")

    def test_invalid_username_returns_401(self) -> None:
        client, _ = self.make_client()

        response = self.post_command(client, {"messt01": "Z"}, username="bad")

        self.assertEqual(response.status_code, 401)

    def test_invalid_password_returns_401(self) -> None:
        client, _ = self.make_client()

        response = self.post_command(client, {"messt01": "Z"}, password="bad")

        self.assertEqual(response.status_code, 401)

    def test_valid_authentication_publishes(self) -> None:
        client, fake_mqtt = self.make_client()

        response = self.post_command(client, {"messt01": "Z106-020C012P001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(fake_mqtt.calls), 1)

    def test_valid_messt01(self) -> None:
        self.assertEqual(
            validate_plc_command({"messt01": "Z106-020C012P001"}),
            {"messt01": "Z106-020C012P001"},
        )

    def test_valid_messt02(self) -> None:
        self.assertEqual(
            validate_plc_command({"messt02": "Z106-020C012P001"}),
            {"messt02": "Z106-020C012P001"},
        )

    def test_valid_messt03(self) -> None:
        self.assertEqual(
            validate_plc_command({"messt03": "Z106-020C012P001"}),
            {"messt03": "Z106-020C012P001"},
        )

    def test_valid_pause_resume_done(self) -> None:
        for command in ({"T01": "P"}, {"T02": "R"}, {"T03": "D"}):
            self.assertEqual(validate_plc_command(command), command)

    def test_valid_loose_packet_command(self) -> None:
        for command in ({"LP": "FT01"}, {"LP": "FT02"}, {"LP": "FT03"}):
            self.assertEqual(validate_plc_command(command), command)

    def test_invalid_command_key(self) -> None:
        with self.assertRaises(CommandValidationError):
            validate_plc_command({"bad": "P"})

    def test_invalid_command_value(self) -> None:
        with self.assertRaises(CommandValidationError):
            validate_plc_command({"T01": "X"})

    def test_mqtt_success_response(self) -> None:
        client, _ = self.make_client()

        response = self.post_command(client, {"messt01": "Z106-020C012P001"})

        body = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(body["success"])
        self.assertEqual(body["status"], "published")
        self.assertEqual(body["topic"], "MQTT/ODOO_TO_PLC/topic")
        self.assertEqual(body["mqtt_mid"], 12)

    def test_mqtt_failure_returns_503(self) -> None:
        client, _ = self.make_client(
            mqtt_client=FakeMqttClient(success=False, error="MQTT broker unavailable")
        )

        response = self.post_command(client, {"messt01": "Z106-020C012P001"})

        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()["success"])
        self.assertEqual(response.json()["status"], "failed")

    def test_validation_failure_returns_422(self) -> None:
        client, _ = self.make_client()

        response = self.post_command(client, {"T01": "X"})

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["status"], "rejected")

    def test_audit_row_created_and_marked_published(self) -> None:
        client, _ = self.make_client()

        response = self.post_command(client, {"messt01": "Z106-020C012P001"})

        record = get_api_command_by_idempotency_key(
            "PRINTJOB-2960-T01",
            self.database_path,
        )
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "PUBLISHED")
        self.assertEqual(record.payload, '{"messt01": "Z106-020C012P001"}')
        self.assertEqual(record.response_code, response.status_code)
        self.assertEqual(record.mqtt_mid, 12)

    def test_failed_row_marked_failed(self) -> None:
        client, _ = self.make_client(mqtt_client=FakeMqttClient(success=False))

        self.post_command(client, {"messt01": "Z106-020C012P001"})

        record = get_api_command_by_idempotency_key(
            "PRINTJOB-2960-T01",
            self.database_path,
        )
        self.assertIsNotNone(record)
        self.assertEqual(record.status, "FAILED")
        self.assertEqual(record.response_code, 503)

    def test_duplicate_idempotency_key_does_not_republish(self) -> None:
        client, fake_mqtt = self.make_client()

        first = self.post_command(client, {"messt01": "Z106-020C012P001"})
        second = self.post_command(client, {"messt01": "Z106-020C012P001"})

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(len(fake_mqtt.calls), 1)

    def test_duplicate_returns_original_response(self) -> None:
        client, _ = self.make_client()

        first = self.post_command(client, {"messt01": "Z106-020C012P001"})
        second = self.post_command(client, {"messt01": "Z106-020C012P001"})

        self.assertEqual(first.json(), second.json())

    def test_duplicate_protection_survives_database_reopen(self) -> None:
        client, fake_mqtt = self.make_client()

        first = self.post_command(client, {"messt01": "Z106-020C012P001"})
        new_client = TestClient(
            create_api_app(make_config(self.database_path), fake_mqtt, self.database_path)
        )
        second = self.post_command(new_client, {"messt01": "Z106-020C012P001"})

        self.assertEqual(first.json(), second.json())
        self.assertEqual(len(fake_mqtt.calls), 1)

    def test_no_idempotency_key_is_accepted(self) -> None:
        client, fake_mqtt = self.make_client()

        response = self.post_command(
            client,
            {"messt01": "Z106-020C012P001"},
            idempotency_key=None,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(fake_mqtt.calls), 1)
        self.assertIsNone(response.json()["idempotency_key"])

    def test_body_size_limit_enforced(self) -> None:
        client, _ = self.make_client(beb_api_max_body_bytes=2)

        response = self.post_command(client, {"messt01": "Z106-020C012P001"})

        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.json()["status"], "rejected")

    def test_api_commands_table_created(self) -> None:
        initialize_api_commands_table(self.database_path)

        with closing(sqlite3.connect(self.database_path)) as connection:
            with connection:
                row = connection.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table' AND name = 'api_commands'
                    """
                ).fetchone()

        self.assertIsNotNone(row)

    def test_dashboard_read_endpoints_are_authenticated_and_bounded(self) -> None:
        saved = save_message(
            topic="MQTT/PLC_TO_ODOO/topic",
            raw_payload='{"MN":"106-020C012P001 7777T01"}',
            message_type="MN",
            table_no="T01",
            model="106-020C012P001",
            serial="7777",
            database_path=self.database_path,
        )
        claim_pending_messages(10, 10, self.database_path)
        mark_completed(
            saved.id,
            json.dumps(
                {
                    "success": True,
                    "result": {
                        "ACK": "7777",
                        "customer_id": 21,
                        "customer_name": "MAGNA",
                    },
                }
            ),
            "2026-07-21T10:00:00+00:00",
            self.database_path,
            ack="7777",
        )
        client, _ = self.make_client()

        unauthenticated = client.get("/api/v1/dashboard/production/recent")
        recent = client.get(
            "/api/v1/dashboard/production/recent?limit=10",
            auth=("odoo", "password"),
        )
        daily = client.get(
            "/api/v1/dashboard/production/daily?limit=10",
            auth=("odoo", "password"),
        )
        summary = client.get(
            "/api/v1/dashboard/production/summary",
            auth=("odoo", "password"),
        )

        self.assertEqual(unauthenticated.status_code, 401)
        self.assertEqual(recent.status_code, 200)
        self.assertEqual(daily.status_code, 200)
        self.assertEqual(summary.status_code, 200)
        self.assertEqual(recent.json()["items"][0]["ack"], "7777")
        self.assertEqual(daily.json()["items"][0]["production_count"], 1)
        self.assertEqual(summary.json()["production_count"], 1)


class TestMqttCommandPublishing(unittest.TestCase):
    def test_correct_plc_json_topic_qos_and_retain(self) -> None:
        expected_payloads = [
            ({"messt01": "S101-025C010P001"}, '{"messt01": "S101-025C010P001"}'),
            ({"messt02": "S101-025C010P001"}, '{"messt02": "S101-025C010P001"}'),
            ({"messt03": "S101-025C010P001"}, '{"messt03": "S101-025C010P001"}'),
            ({"T01": "P"}, '{"T01": "P"}'),
            ({"LP": "FT01"}, '{"LP": "FT01"}'),
        ]

        for command, expected_payload in expected_payloads:
            with self.subTest(command=command):
                config = make_config(Path("data/test_unused.db"))
                client = BEBMqttClient.__new__(BEBMqttClient)
                fake_paho = MagicMock()
                fake_paho.is_connected.return_value = True
                fake_paho.publish.return_value = SimpleNamespace(
                    rc=mqtt.MQTT_ERR_SUCCESS,
                    mid=99,
                )
                client._config = config
                client._client = fake_paho

                result = client.publish_plc_command(command)

                self.assertTrue(result.success)
                self.assertEqual(result.payload, expected_payload)
                self.assertEqual(result.topic, "MQTT/ODOO_TO_PLC/topic")
                fake_paho.publish.assert_called_once_with(
                    "MQTT/ODOO_TO_PLC/topic",
                    payload=expected_payload,
                    qos=0,
                    retain=False,
                )

    def test_publish_fails_cleanly_when_mqtt_disconnected(self) -> None:
        config = make_config(Path("data/test_unused.db"))
        client = BEBMqttClient.__new__(BEBMqttClient)
        fake_paho = MagicMock()
        fake_paho.is_connected.return_value = False
        client._config = config
        client._client = fake_paho

        result = client.publish_plc_command({"messt01": "Z106-020C012P001"})

        self.assertFalse(result.success)
        self.assertEqual(result.error, "MQTT broker unavailable")
        fake_paho.publish.assert_not_called()


class TestApiServerLifecycle(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path.cwd() / "data"
        temp_root.mkdir(parents=True, exist_ok=True)
        self.database_path = temp_root / f"test_server_{uuid.uuid4().hex}.db"

    def tearDown(self) -> None:
        for path in (
            self.database_path,
            self.database_path.with_name(f"{self.database_path.name}-wal"),
            self.database_path.with_name(f"{self.database_path.name}-shm"),
        ):
            if path.exists():
                path.unlink()

    def test_api_server_disabled_behavior(self) -> None:
        config = make_config(self.database_path, beb_api_enabled=False)
        server = BebApiServer(config, FakeMqttClient(), config.database_path)

        server.start()

        self.assertFalse(server.is_running())

    def test_api_server_clean_startup_and_shutdown(self) -> None:
        config = make_config(self.database_path, beb_api_enabled=True)

        class FakeServer:
            def __init__(self, uvicorn_config) -> None:
                self.should_exit = False

            def run(self) -> None:
                while not self.should_exit:
                    time.sleep(0.01)

        with patch("app.api_server.uvicorn.Server", FakeServer):
            server = BebApiServer(config, FakeMqttClient(), config.database_path)
            server.start()
            time.sleep(0.05)
            self.assertTrue(server.is_running())
            server.stop()

        self.assertFalse(server.is_running())


if __name__ == "__main__":
    unittest.main()
