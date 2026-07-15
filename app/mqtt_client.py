"""MQTT client wrapper for the BlackBear Edge Bridge."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any, Callable

import paho.mqtt.client as mqtt

from app.config import AppConfig
from app.message_parser import PLCMessageParseError, ParsedPLCMessage, parse_plc_message

LOGGER = logging.getLogger(__name__)
QOS = 0


MessageHandler = Callable[[ParsedPLCMessage], None]


class BEBMqttClient:
    """Small production-oriented wrapper around paho-mqtt."""

    def __init__(
        self,
        config: AppConfig,
        message_handler: MessageHandler | None = None,
    ) -> None:
        self._config = config
        self._message_handler = message_handler or self._default_message_handler
        self._client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=config.mqtt_client_id,
            protocol=mqtt.MQTTv311,
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.reconnect_delay_set(min_delay=1, max_delay=30)

    def connect(self) -> None:
        LOGGER.info(
            "Connecting to MQTT broker %s:%s as %s",
            self._config.mqtt_host,
            self._config.mqtt_port,
            self._config.mqtt_client_id,
        )
        self._client.connect(
            self._config.mqtt_host,
            self._config.mqtt_port,
            self._config.mqtt_keepalive,
        )

    def run_forever(self) -> None:
        self.connect()
        self._client.loop_forever(retry_first_connection=True)

    def shutdown(self) -> None:
        LOGGER.info("Shutting down MQTT client")
        self._client.disconnect()
        self._client.loop_stop()

    def publish_odoo_command(self, command: Mapping[str, Any] | str | bytes) -> None:
        """Publish an Odoo-style command payload to the PLC command topic."""
        payload: str | bytes
        if isinstance(command, Mapping):
            payload = json.dumps(command, separators=(",", ":"))
        else:
            payload = command

        info = self._client.publish(
            self._config.mqtt_odoo_to_plc_topic,
            payload=payload,
            qos=QOS,
            retain=False,
        )
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error(
                "Failed to publish command to %s: rc=%s",
                self._config.mqtt_odoo_to_plc_topic,
                info.rc,
            )
            return

        LOGGER.info("Published command to %s", self._config.mqtt_odoo_to_plc_topic)

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: mqtt.ConnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        if reason_code.is_failure:
            LOGGER.error("MQTT connection failed: %s", reason_code)
            return

        LOGGER.info("Connected to MQTT broker")
        result, message_id = client.subscribe(self._config.mqtt_plc_to_odoo_topic, qos=QOS)
        if result != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.error(
                "Failed to subscribe to %s: rc=%s",
                self._config.mqtt_plc_to_odoo_topic,
                result,
            )
            return

        LOGGER.info(
            "Subscribed to %s with QoS %s (mid=%s)",
            self._config.mqtt_plc_to_odoo_topic,
            QOS,
            message_id,
        )

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: mqtt.DisconnectFlags,
        reason_code: mqtt.ReasonCode,
        properties: mqtt.Properties | None,
    ) -> None:
        if reason_code.is_failure:
            LOGGER.warning("Unexpected MQTT disconnect: %s. Reconnecting automatically.", reason_code)
        else:
            LOGGER.info("MQTT client disconnected")

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        try:
            parsed = parse_plc_message(message.payload)
        except PLCMessageParseError as exc:
            LOGGER.warning("Rejected PLC message on %s: %s", message.topic, exc)
            return

        LOGGER.info(
            "Received PLC message: type=%s part_data=%s serial=%s table=%s",
            parsed.message_type,
            parsed.part_data,
            parsed.serial,
            parsed.table,
        )
        self._message_handler(parsed)

    @staticmethod
    def _default_message_handler(message: ParsedPLCMessage) -> None:
        LOGGER.debug("No downstream handler configured for parsed PLC message: %s", message)
