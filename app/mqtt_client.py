"""MQTT client wrapper for the BlackBear Edge Bridge."""

from __future__ import annotations

import ast
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import paho.mqtt.client as mqtt

from app.config import AppConfig
from app.database import (
    SavedMessage,
    get_message_by_hash,
    get_message_by_id,
    record_ack_replay_attempt,
    save_message,
)
from app.message_parser import PLCMessageParseError, ParsedPLCMessage, parse_plc_message

LOGGER = logging.getLogger(__name__)
QOS = 0
PLC_JSON_SEPARATORS = (",", ": ")


MessageHandler = Callable[[ParsedPLCMessage], None]
MESSAGE_TYPE_DESCRIPTIONS = {
    "MN": "Print Completed",
    "MP": "Loose Packet",
}


@dataclass(frozen=True, slots=True)
class PublishResult:
    success: bool
    rc: int
    mid: int | None
    topic: str
    payload: str
    error: str | None = None


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

    def start_loop(self) -> None:
        """Start the MQTT network loop without blocking the main thread."""
        LOGGER.info(
            "Starting MQTT loop for broker %s:%s as %s",
            self._config.mqtt_host,
            self._config.mqtt_port,
            self._config.mqtt_client_id,
        )
        self._client.connect_async(
            self._config.mqtt_host,
            self._config.mqtt_port,
            self._config.mqtt_keepalive,
        )
        self._client.loop_start()

    def shutdown(self) -> None:
        LOGGER.info("Shutting down MQTT client")
        self._client.disconnect()
        self._client.loop_stop()

    def is_connected(self) -> bool:
        """Return whether the MQTT client currently reports a broker connection."""
        return bool(self._client.is_connected())

    def publish_odoo_command(self, command: Mapping[str, Any] | str | bytes) -> None:
        """Publish an Odoo-style command payload to the PLC command topic."""
        payload: str | bytes
        if isinstance(command, Mapping):
            payload = json.dumps(command, separators=PLC_JSON_SEPARATORS)
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

    def publish_plc_command(self, payload: dict[str, str]) -> PublishResult:
        """Publish a validated PLC command to the configured command topic."""
        topic = self._config.mqtt_odoo_to_plc_topic
        payload_text = json.dumps(payload, separators=PLC_JSON_SEPARATORS)

        try:
            if not self._client.is_connected():
                return PublishResult(
                    success=False,
                    rc=mqtt.MQTT_ERR_NO_CONN,
                    mid=None,
                    topic=topic,
                    payload=payload_text,
                    error="MQTT broker unavailable",
                )

            info = self._client.publish(
                topic,
                payload=payload_text,
                qos=QOS,
                retain=False,
            )
        except Exception as exc:
            return PublishResult(
                success=False,
                rc=mqtt.MQTT_ERR_UNKNOWN,
                mid=None,
                topic=topic,
                payload=payload_text,
                error=str(exc),
            )

        rc = int(info.rc)
        success = rc == mqtt.MQTT_ERR_SUCCESS
        return PublishResult(
            success=success,
            rc=rc,
            mid=int(info.mid) if getattr(info, "mid", None) is not None else None,
            topic=topic,
            payload=payload_text,
            error=None if success else f"MQTT publish rc={rc}",
        )

    def publish_ack(self, ack: str) -> bool:
        """Publish an ACK payload to the PLC command topic."""
        return publish_ack(self._client, self._config.mqtt_odoo_to_plc_topic, ack)

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
        raw_payload = _decode_payload_for_log(message.payload)
        try:
            parsed = parse_plc_message(message.payload)
        except PLCMessageParseError as exc:
            LOGGER.warning(
                "Rejected PLC message on %s: %s. Raw payload: %s",
                message.topic,
                exc,
                raw_payload,
            )
            return

        saved_message = save_message(
            topic=message.topic,
            raw_payload=raw_payload,
            message_type=parsed.message_type,
            table_no=parsed.table_number,
            model=parsed.model_number,
            serial=parsed.serial_number,
            database_path=self._config.database_path,
        )
        if not saved_message.inserted:
            self._handle_duplicate_message(saved_message)
            return

        LOGGER.info(
            "\n%s",
            format_received_message_log(message.topic, raw_payload, parsed, saved_message),
        )
        self._message_handler(parsed)

    @staticmethod
    def _default_message_handler(message: ParsedPLCMessage) -> None:
        LOGGER.debug("No downstream handler configured for parsed PLC message: %s", message)

    def _handle_duplicate_message(self, saved_message: SavedMessage) -> None:
        record = get_message_by_id(saved_message.id, self._config.database_path)
        if record is None:
            record = get_message_by_hash(
                saved_message.message_hash,
                self._config.database_path,
            )
        if record is None:
            LOGGER.warning(
                "Duplicate ACK replay suppressed: original message not found hash=%s",
                saved_message.message_hash,
            )
            return

        if record.status != "COMPLETED":
            LOGGER.info(
                "Duplicate ACK replay suppressed: message id=%s hash=%s status=%s",
                record.id,
                record.message_hash,
                record.status,
            )
            return

        response = parse_stored_odoo_response(record.odoo_response)
        if response is None:
            LOGGER.warning(
                "Duplicate ACK replay invalid response: message id=%s hash=%s",
                record.id,
                record.message_hash,
            )
            return

        if response.get("success") is not True:
            LOGGER.warning(
                "Duplicate ACK replay invalid response: message id=%s hash=%s success=%r",
                record.id,
                record.message_hash,
                response.get("success"),
            )
            return

        ack = extract_stored_ack(response)
        if ack is None:
            LOGGER.warning(
                "Duplicate ACK replay invalid response: message id=%s hash=%s missing ACK",
                record.id,
                record.message_hash,
            )
            return

        attempt = record_ack_replay_attempt(
            record.message_hash,
            self._config.database_path,
        )
        if not attempt.allowed:
            if attempt.reason == "minimum-interval":
                LOGGER.info(
                    "Duplicate ACK replay suppressed by minimum interval: id=%s hash=%s count=%s",
                    record.id,
                    record.message_hash,
                    attempt.replay_count,
                )
                return
            LOGGER.warning(
                "Duplicate ACK replay limit reached: id=%s hash=%s count=%s",
                record.id,
                record.message_hash,
                attempt.replay_count,
            )
            return

        LOGGER.info(
            "Replaying stored ACK for duplicate message: id=%s hash=%s ack=%s count=%s",
            record.id,
            record.message_hash,
            ack,
            attempt.replay_count,
        )
        self.publish_ack(ack)


def format_received_message_log(
    topic: str,
    raw_payload: str,
    parsed: ParsedPLCMessage,
    saved_message: SavedMessage | None = None,
    timestamp: datetime | None = None,
) -> str:
    """Build the formatted MQTT receive log block."""
    received_at = timestamp or datetime.now().astimezone()
    message_description = MESSAGE_TYPE_DESCRIPTIONS.get(parsed.message_type, "Unknown")

    lines = [
        "-" * 50,
        "Received MQTT Message",
        f"Timestamp  : {received_at.isoformat(timespec='seconds')}",
        f"Topic      : {topic}",
        f"Raw Payload: {raw_payload}",
        f"Type       : {parsed.message_type} ({message_description})",
        f"Table      : {parsed.table_number}",
        f"Model      : {parsed.model_number}",
        f"Serial     : {parsed.serial_number}",
    ]
    if saved_message is not None:
        lines.extend(
            [
                "Saved to SQLite",
                f"ID         : {saved_message.id}",
                f"Hash       : {saved_message.message_hash}",
                f"Status     : {saved_message.status}",
            ]
        )
    lines.append("-" * 50)
    return "\n".join(lines)


def publish_ack(client: mqtt.Client, topic: str, ack: str) -> bool:
    """Publish an Odoo ACK response back to the PLC."""
    payload = json.dumps({"ACK": ack}, separators=PLC_JSON_SEPARATORS)
    LOGGER.info("\n%s", format_ack_publish_log(ack, topic))

    try:
        info = client.publish(topic, payload=payload, qos=QOS, retain=False)
    except Exception as exc:
        LOGGER.error("\n%s", format_ack_publish_failure_log(str(exc)))
        return False

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        LOGGER.error("\n%s", format_ack_publish_failure_log(f"MQTT publish rc={info.rc}"))
        return False

    LOGGER.info("\n%s", format_ack_publish_success_log(ack))
    return True


def format_ack_publish_log(ack: str, topic: str) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Publishing ACK to PLC",
            f"ACK        : {ack}",
            f"Topic      : {topic}",
            "-" * 50,
        ]
    )


def format_ack_publish_success_log(ack: str) -> str:
    return "\n".join(
        [
            "-" * 50,
            "ACK Published Successfully",
            f"ACK : {ack}",
            "-" * 50,
        ]
    )


def format_ack_publish_failure_log(reason: str) -> str:
    return "\n".join(
        [
            "-" * 50,
            "ACK Publish Failed",
            f"Reason : {reason}",
            "-" * 50,
        ]
    )


def _decode_payload_for_log(payload: bytes) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError:
        return repr(payload)


def parse_stored_odoo_response(response_text: str | None) -> dict[str, Any] | None:
    """Parse valid JSON or legacy Python repr Odoo responses from SQLite."""
    if not response_text:
        return None

    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(response_text)
        except (SyntaxError, ValueError):
            return None

    return parsed if isinstance(parsed, dict) else None


def extract_stored_ack(response: Mapping[str, Any]) -> str | None:
    result = response.get("result")
    if not isinstance(result, Mapping):
        return None

    ack = result.get("ACK")
    return ack if isinstance(ack, str) and ack else None
