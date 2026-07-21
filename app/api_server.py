"""Programmatic Uvicorn server lifecycle for the BEB API."""

from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

import uvicorn

from app.api import create_api_app
from app.config import AppConfig


LOGGER = logging.getLogger(__name__)


class BebApiServer:
    """Run the BEB FastAPI app in a dedicated thread."""

    def __init__(
        self,
        config: AppConfig,
        mqtt_client: Any,
        database_path: str | Path,
        odoo_worker: Any | None = None,
        readiness_monitor: Any | None = None,
    ) -> None:
        self._config = config
        self._mqtt_client = mqtt_client
        self._database_path = database_path
        self._odoo_worker = odoo_worker
        self._readiness_monitor = readiness_monitor
        self._thread: threading.Thread | None = None
        self._server: uvicorn.Server | None = None
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the API server if enabled."""
        if not self._config.beb_api_enabled:
            LOGGER.info("BEB API disabled")
            return

        with self._lock:
            if self.is_running():
                LOGGER.warning("BEB API server is already running")
                return

            app = create_api_app(
                self._config,
                self._mqtt_client,
                self._database_path,
                self._odoo_worker,
                self._readiness_monitor,
            )
            uvicorn_config = uvicorn.Config(
                app,
                host=self._config.beb_api_host,
                port=self._config.beb_api_port,
                log_level=self._config.log_level.lower(),
            )
            self._server = uvicorn.Server(uvicorn_config)
            self._thread = threading.Thread(
                target=self._run,
                name="beb-api-server",
                daemon=True,
            )
            self._thread.start()
            LOGGER.info(
                "BEB API server starting at http://%s:%s",
                self._config.beb_api_host,
                self._config.beb_api_port,
            )

    def stop(self) -> None:
        """Stop the API server if it is running."""
        with self._lock:
            server = self._server
            thread = self._thread
            if server is None or thread is None:
                return

            server.should_exit = True
            if thread.is_alive():
                thread.join(timeout=10)
            self._server = None
            self._thread = None
            LOGGER.info("BEB API server stopped")

    def is_running(self) -> bool:
        """Return whether the API thread is alive."""
        return self._thread is not None and self._thread.is_alive()

    def _run(self) -> None:
        try:
            if self._server is not None:
                self._server.run()
        except Exception:
            LOGGER.exception("BEB API server stopped unexpectedly")
