"""Application entry point for the BlackBear Edge Bridge."""

from __future__ import annotations

import logging

from app.config import AppConfig, load_config
from app.database import initialize_database
from app.mqtt_client import BEBMqttClient


def configure_logging(config: AppConfig) -> None:
    logging.basicConfig(
        level=getattr(logging, config.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def main() -> None:
    config = load_config()
    configure_logging(config)
    initialize_database(config.database_path)

    client = BEBMqttClient(config)
    try:
        client.run_forever()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Ctrl+C received")
    finally:
        client.shutdown()


if __name__ == "__main__":
    main()
