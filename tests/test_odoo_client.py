from __future__ import annotations

import socket
import unittest
import xmlrpc.client
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from app.odoo_client import (
    OdooAuthenticationError,
    OdooClientSettings,
    OdooSubmissionError,
    OdooXmlRpcClient,
    _TimeoutSafeTransport,
)


class TestOdooXmlRpcClient(unittest.TestCase):
    def setUp(self) -> None:
        self.client = OdooXmlRpcClient(
            url="https://test-bbw.odoo.com",
            database="broadtechit-test-bbw-stage-34933250",
            username="admin",
            password="secret",
            model="iot.configuration",
            submit_method="xmlrpc_submit_print_data",
            timeout=15,
        )

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_authentication_success_and_common_endpoint(self, server_proxy) -> None:
        common = MagicMock()
        common.authenticate.return_value = 7
        server_proxy.return_value = common

        uid = self.client.authenticate()

        self.assertEqual(uid, 7)
        server_proxy.assert_called_once()
        self.assertEqual(
            server_proxy.call_args.args[0],
            "https://test-bbw.odoo.com/xmlrpc/2/common",
        )
        self.assertTrue(server_proxy.call_args.kwargs["allow_none"])
        self.assertIsInstance(
            server_proxy.call_args.kwargs["transport"],
            _TimeoutSafeTransport,
        )
        common.authenticate.assert_called_once_with(
            "broadtechit-test-bbw-stage-34933250",
            "admin",
            "secret",
            {},
        )

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_authentication_failure(self, server_proxy) -> None:
        common = MagicMock()
        common.authenticate.return_value = False
        server_proxy.return_value = common

        with self.assertRaises(OdooAuthenticationError):
            self.client.authenticate()

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_submit_uses_object_endpoint_model_method_database_and_payload(self, server_proxy) -> None:
        common = MagicMock()
        common.authenticate.return_value = 7
        models = MagicMock()
        models.execute_kw.return_value = {"ok": True}
        server_proxy.side_effect = [common, models]

        result = self.client.submit_print_data({"MN": "106-020C012P001 3242T01"})

        self.assertEqual(result, {"ok": True})
        self.assertEqual(
            server_proxy.call_args_list[1].args[0],
            "https://test-bbw.odoo.com/xmlrpc/2/object",
        )
        self.assertIsInstance(
            server_proxy.call_args_list[0].kwargs["transport"],
            _TimeoutSafeTransport,
        )
        self.assertIsInstance(
            server_proxy.call_args_list[1].kwargs["transport"],
            _TimeoutSafeTransport,
        )
        self.assertTrue(server_proxy.call_args_list[0].kwargs["allow_none"])
        self.assertTrue(server_proxy.call_args_list[1].kwargs["allow_none"])
        models.execute_kw.assert_called_once_with(
            "broadtechit-test-bbw-stage-34933250",
            7,
            "secret",
            "iot.configuration",
            "xmlrpc_submit_print_data",
            [{"MN": "106-020C012P001 3242T01"}],
        )

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_exact_mp_payload_passed_unchanged(self, server_proxy) -> None:
        common = MagicMock()
        common.authenticate.return_value = 7
        models = MagicMock()
        models.execute_kw.return_value = True
        server_proxy.side_effect = [common, models]
        payload = {"MP": "Z106-015C020P001 7084T01"}

        self.client.submit_print_data(payload)

        self.assertEqual(models.execute_kw.call_args.args[5], [payload])

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_xmlrpc_fault_handling(self, server_proxy) -> None:
        common = MagicMock()
        common.authenticate.return_value = 7
        models = MagicMock()
        models.execute_kw.side_effect = xmlrpc.client.Fault(100, "boom")
        server_proxy.side_effect = [common, models]

        with self.assertRaises(OdooSubmissionError):
            self.client.submit_print_data({"MN": "106-020C012P001 3242T01"})

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_protocol_error_handling(self, server_proxy) -> None:
        common = MagicMock()
        common.authenticate.return_value = 7
        models = MagicMock()
        models.execute_kw.side_effect = xmlrpc.client.ProtocolError(
            "https://test-bbw.odoo.com/xmlrpc/2/object",
            500,
            "Server Error",
            {},
        )
        server_proxy.side_effect = [common, models]

        with self.assertRaises(OdooSubmissionError):
            self.client.submit_print_data({"MN": "106-020C012P001 3242T01"})

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_timeout_handling(self, server_proxy) -> None:
        common = MagicMock()
        common.authenticate.return_value = 7
        models = MagicMock()
        models.execute_kw.side_effect = socket.timeout("timed out")
        server_proxy.side_effect = [common, models]

        with self.assertRaises(OdooSubmissionError):
            self.client.submit_print_data({"MN": "106-020C012P001 3242T01"})

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_reauthentication_after_session_failure(self, server_proxy) -> None:
        common = MagicMock()
        common.authenticate.side_effect = [7, 8]
        models = MagicMock()
        models.execute_kw.side_effect = [
            xmlrpc.client.Fault(1, "Access denied: session expired"),
            {"ok": True},
        ]
        server_proxy.side_effect = [common, models]

        result = self.client.submit_print_data({"MN": "106-020C012P001 3242T01"})

        self.assertEqual(result, {"ok": True})
        self.assertEqual(common.authenticate.call_count, 2)
        self.assertEqual(models.execute_kw.call_count, 2)
        self.assertEqual(models.execute_kw.call_args.args[1], 8)

    def test_close_performs_no_xmlrpc_call_and_clears_state(self) -> None:
        common = MagicMock()
        models = MagicMock()
        self.client._common = common
        self.client._models = models
        self.client._uid = 7
        self.client._last_authenticated_at = datetime.now(timezone.utc)

        self.client.close()

        common.assert_not_called()
        models.assert_not_called()
        self.assertIsNone(self.client._common)
        self.assertIsNone(self.client._models)
        self.assertIsNone(self.client._uid)
        self.assertIsNone(self.client._last_authenticated_at)

    def test_close_is_idempotent(self) -> None:
        self.client._common = MagicMock()
        self.client._models = MagicMock()
        self.client._uid = 7

        self.client.close()
        self.client.close()

        self.assertIsNone(self.client._common)
        self.assertIsNone(self.client._models)
        self.assertIsNone(self.client._uid)

    def test_settings_are_serializable_for_worker_child(self) -> None:
        settings = self.client.settings()

        self.assertEqual(
            settings,
            OdooClientSettings(
                url="https://test-bbw.odoo.com",
                database="broadtechit-test-bbw-stage-34933250",
                username="admin",
                password="secret",
                model="iot.configuration",
                submit_method="xmlrpc_submit_print_data",
                timeout=15,
            ),
        )

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_protocol_error_resets_cached_proxies(self, server_proxy) -> None:
        common = MagicMock()
        common.authenticate.return_value = 7
        models = MagicMock()
        models.execute_kw.side_effect = xmlrpc.client.ProtocolError(
            "https://test-bbw.odoo.com/xmlrpc/2/object",
            500,
            "Server Error",
            {},
        )
        server_proxy.side_effect = [common, models]

        with self.assertRaises(OdooSubmissionError):
            self.client.submit_print_data({"MN": "106-020C012P001 3242T01"})

        self.assertIsNone(self.client._common)
        self.assertIsNone(self.client._models)
        self.assertIsNone(self.client._uid)

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_readiness_check_uses_version_and_authentication(self, server_proxy) -> None:
        common = MagicMock()
        common.version.return_value = {"server_version": "18.0"}
        common.authenticate.return_value = 7
        server_proxy.return_value = common

        self.assertTrue(self.client.check_readiness())

        common.version.assert_called_once_with()
        common.authenticate.assert_called_once_with(
            "broadtechit-test-bbw-stage-34933250",
            "admin",
            "secret",
            {},
        )

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_readiness_check_reuses_cached_authentication_when_healthy(self, server_proxy) -> None:
        common = MagicMock()
        common.version.return_value = {"server_version": "18.0"}
        common.authenticate.return_value = 7
        server_proxy.return_value = common

        self.assertTrue(self.client.check_readiness(revalidate_after_seconds=300))
        self.assertTrue(self.client.check_readiness(revalidate_after_seconds=300))

        self.assertEqual(common.version.call_count, 2)
        self.assertEqual(common.authenticate.call_count, 1)

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_readiness_check_reauthenticates_after_session_invalidation(self, server_proxy) -> None:
        common = MagicMock()
        common.version.return_value = {"server_version": "18.0"}
        common.authenticate.side_effect = [7, 8]
        server_proxy.return_value = common

        self.assertTrue(self.client.check_readiness(revalidate_after_seconds=300))
        self.client.invalidate_session()
        self.assertTrue(self.client.check_readiness(revalidate_after_seconds=300))

        self.assertEqual(common.authenticate.call_count, 2)
        self.assertEqual(self.client._uid, 8)

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_readiness_check_reauthenticates_after_revalidation_interval(self, server_proxy) -> None:
        common = MagicMock()
        common.version.return_value = {"server_version": "18.0"}
        common.authenticate.side_effect = [7, 8]
        server_proxy.return_value = common

        self.assertTrue(self.client.check_readiness(revalidate_after_seconds=300))
        self.client._last_authenticated_at = datetime.now(timezone.utc) - timedelta(
            seconds=301
        )
        self.assertTrue(self.client.check_readiness(revalidate_after_seconds=300))

        self.assertEqual(common.authenticate.call_count, 2)
        self.assertEqual(self.client._uid, 8)

    @patch("app.odoo_client.xmlrpc.client.ServerProxy")
    def test_readiness_check_resets_proxy_after_transport_failure(self, server_proxy) -> None:
        common = MagicMock()
        common.version.side_effect = socket.timeout("timed out")
        server_proxy.return_value = common

        with self.assertRaises(OdooAuthenticationError):
            self.client.check_readiness()

        self.assertIsNone(self.client._common)
        self.assertIsNone(self.client._models)
        self.assertIsNone(self.client._uid)


if __name__ == "__main__":
    unittest.main()
