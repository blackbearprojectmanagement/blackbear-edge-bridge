"""Odoo XML-RPC client for forwarding PLC print data."""

from __future__ import annotations

import logging
import socket
import ssl
from dataclasses import dataclass
from typing import Any
import xmlrpc.client


LOGGER = logging.getLogger(__name__)


class OdooAuthenticationError(Exception):
    """Raised when Odoo authentication fails."""


class OdooSubmissionError(Exception):
    """Raised when submitting print data to Odoo fails."""


@dataclass(frozen=True, slots=True)
class OdooClientSettings:
    url: str
    database: str
    username: str
    password: str
    model: str
    submit_method: str
    timeout: int


class OdooXmlRpcClient:
    """Authenticated XML-RPC client for the confirmed Odoo model and method."""

    def __init__(
        self,
        url: str,
        database: str,
        username: str,
        password: str,
        model: str,
        submit_method: str,
        timeout: int = 15,
    ) -> None:
        self._url = url.rstrip("/")
        self._database = database
        self._username = username
        self._password = password
        self._model = model
        self._submit_method = submit_method
        self._timeout = timeout
        self._uid: int | None = None
        self._common: xmlrpc.client.ServerProxy | None = None
        self._models: xmlrpc.client.ServerProxy | None = None

    @property
    def timeout(self) -> int:
        return self._timeout

    def settings(self) -> OdooClientSettings:
        """Return serializable settings for isolated subprocess submissions."""
        return OdooClientSettings(
            url=self._url,
            database=self._database,
            username=self._username,
            password=self._password,
            model=self._model,
            submit_method=self._submit_method,
            timeout=self._timeout,
        )

    @classmethod
    def from_settings(cls, settings: OdooClientSettings) -> OdooXmlRpcClient:
        return cls(
            url=settings.url,
            database=settings.database,
            username=settings.username,
            password=settings.password,
            model=settings.model,
            submit_method=settings.submit_method,
            timeout=settings.timeout,
        )

    def authenticate(self) -> int:
        """Authenticate with Odoo and return the numeric UID."""
        try:
            common = self._get_common_proxy()
            uid = common.authenticate(
                self._database,
                self._username,
                self._password,
                {},
            )
        except _ODOO_TRANSPORT_ERRORS as exc:
            self.close()
            raise OdooAuthenticationError(f"Odoo authentication failed: {exc}") from exc

        if not isinstance(uid, int) or uid <= 0:
            self.close()
            raise OdooAuthenticationError("Odoo authentication failed: invalid UID")

        self._uid = uid
        LOGGER.info("Authenticated with Odoo as user %s", self._username)
        return uid

    def submit_print_data(self, payload: dict[str, str]) -> object:
        """Submit one original PLC payload to Odoo."""
        try:
            return self._submit_print_data(payload, allow_reauth=True)
        except OdooSubmissionError:
            raise
        except OdooAuthenticationError as exc:
            raise OdooSubmissionError(str(exc)) from exc

    def check_readiness(self) -> bool:
        """Verify Odoo XML-RPC and database authentication without business effects."""
        try:
            common = self._get_common_proxy()
            version = common.version()
            if not isinstance(version, dict):
                return False
            self.authenticate()
            return True
        except OdooAuthenticationError:
            raise
        except _ODOO_TRANSPORT_ERRORS as exc:
            self.close()
            raise OdooAuthenticationError(f"Odoo readiness check failed: {exc}") from exc

    def is_authenticated(self) -> bool:
        return self._uid is not None and self._uid > 0

    def invalidate_session(self) -> None:
        self._uid = None

    def reset_session(self) -> None:
        """Drop cached proxies and authentication state after unsafe transport errors."""
        self.close()

    def close(self) -> None:
        self._common = None
        self._models = None
        self.invalidate_session()

    def _submit_print_data(
        self,
        payload: dict[str, str],
        *,
        allow_reauth: bool,
    ) -> object:
        if not self.is_authenticated():
            self.authenticate()

        try:
            models = self._get_models_proxy()
            return models.execute_kw(
                self._database,
                self._uid,
                self._password,
                self._model,
                self._submit_method,
                [payload],
            )
        except xmlrpc.client.Fault as exc:
            if allow_reauth and _is_authentication_fault(exc):
                self.invalidate_session()
                self.authenticate()
                return self._submit_print_data(payload, allow_reauth=False)
            raise OdooSubmissionError(f"Odoo XML-RPC fault: {exc}") from exc
        except xmlrpc.client.ProtocolError as exc:
            self.close()
            raise OdooSubmissionError(
                f"Odoo XML-RPC protocol error: {exc.errcode} {exc.errmsg}"
            ) from exc
        except _ODOO_TRANSPORT_ERRORS as exc:
            self.close()
            raise OdooSubmissionError(f"Odoo XML-RPC transport error: {exc}") from exc

    def _get_common_proxy(self) -> xmlrpc.client.ServerProxy:
        if self._common is None:
            self._common = xmlrpc.client.ServerProxy(
                f"{self._url}/xmlrpc/2/common",
                transport=_make_timeout_transport(self._url, self._timeout),
                allow_none=True,
            )
        return self._common

    def _get_models_proxy(self) -> xmlrpc.client.ServerProxy:
        if self._models is None:
            self._models = xmlrpc.client.ServerProxy(
                f"{self._url}/xmlrpc/2/object",
                transport=_make_timeout_transport(self._url, self._timeout),
                allow_none=True,
            )
        return self._models


_ODOO_TRANSPORT_ERRORS = (
    xmlrpc.client.ProtocolError,
    socket.timeout,
    TimeoutError,
    ConnectionError,
    OSError,
    ssl.SSLError,
)


def _is_authentication_fault(exc: xmlrpc.client.Fault) -> bool:
    text = f"{exc.faultCode} {exc.faultString}".lower()
    return any(
        marker in text
        for marker in (
            "access denied",
            "authentication",
            "session",
            "login",
            "password",
            "uid",
        )
    )


class _TimeoutTransport(xmlrpc.client.Transport):
    def __init__(self, timeout: int) -> None:
        super().__init__()
        self._timeout = timeout

    def make_connection(self, host: str) -> Any:
        connection = super().make_connection(host)
        connection.timeout = self._timeout
        return connection


class _TimeoutSafeTransport(xmlrpc.client.SafeTransport):
    def __init__(self, timeout: int) -> None:
        super().__init__()
        self._timeout = timeout

    def make_connection(self, host: str) -> Any:
        connection = super().make_connection(host)
        connection.timeout = self._timeout
        return connection


def _make_timeout_transport(
    url: str,
    timeout: int,
) -> xmlrpc.client.Transport | xmlrpc.client.SafeTransport:
    if url.lower().startswith("https://"):
        return _TimeoutSafeTransport(timeout)
    return _TimeoutTransport(timeout)
