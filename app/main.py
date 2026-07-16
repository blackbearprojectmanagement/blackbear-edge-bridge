"""Application entry point for the BlackBear Edge Bridge."""

from __future__ import annotations

import logging
import threading
from typing import Callable

from app.api_server import BebApiServer
from app.config import AppConfig, load_config
from app.database import initialize_api_commands_table, initialize_database
from app.mqtt_client import BEBMqttClient
from app.odoo_client import OdooXmlRpcClient
from app.queue_worker import OdooQueueWorker


def configure_logging(config: AppConfig) -> None:
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def create_odoo_worker(
    config: AppConfig,
    ack_publisher: Callable[[str], bool] | None = None,
) -> tuple[OdooQueueWorker, OdooXmlRpcClient] | None:
    if not config.odoo_enabled:
        logging.getLogger(__name__).info(
            "Odoo integration disabled; messages will remain in SQLite with status NEW."
        )
        return None

    odoo_client = OdooXmlRpcClient(
        url=config.odoo_url,
        database=config.odoo_database,
        username=config.odoo_username,
        password=config.odoo_password,
        model=config.odoo_model,
        submit_method=config.odoo_submit_method,
        timeout=config.odoo_timeout,
    )
    worker = OdooQueueWorker(
        database_path=config.database_path,
        odoo_client=odoo_client,
        worker_interval=config.odoo_worker_interval,
        batch_size=config.odoo_batch_size,
        max_retries=config.odoo_max_retries,
        stale_processing_timeout=config.odoo_stale_processing_seconds,
        ack_publisher=ack_publisher,
    )
    return worker, odoo_client


def main() -> None:
    config = load_config()
    configure_logging(config)
    initialize_database(config.database_path)
    initialize_api_commands_table(config.database_path)

    client = BEBMqttClient(config)
    worker_bundle = create_odoo_worker(config, ack_publisher=client.publish_ack)
    worker: OdooQueueWorker | None = None
    odoo_client: OdooXmlRpcClient | None = None
    if worker_bundle is not None:
        worker, odoo_client = worker_bundle
        worker.start()

    api_server = BebApiServer(config, client, config.database_path)

    try:
        client.start_loop()
        api_server.start()
        threading.Event().wait()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Ctrl+C received")
    finally:
        api_server.stop()
        if worker is not None:
            worker.stop()
        client.shutdown()
        if odoo_client is not None:
            odoo_client.close()


if __name__ == "__main__":
    main()
