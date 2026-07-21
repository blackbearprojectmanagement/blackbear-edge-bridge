"""Authenticated HTTP API for Odoo-to-PLC commands."""

from __future__ import annotations

import json
import logging
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.command_parser import CommandValidationError, validate_plc_command
from app.config import AppConfig
from app.database import (
    ApiCommandRecord,
    create_api_command_record,
    get_api_command_by_idempotency_key,
    get_queue_counts,
    initialize_api_commands_table,
    mark_api_command_failed,
    mark_api_command_published,
    mark_api_command_rejected,
    save_api_response,
)
from app.mqtt_client import PLC_JSON_SEPARATORS, PublishResult


LOGGER = logging.getLogger(__name__)
SECURITY = HTTPBasic(auto_error=False)


def create_api_app(
    config: AppConfig,
    mqtt_client: Any,
    database_path: str | Path,
    odoo_worker: Any | None = None,
    readiness_monitor: Any | None = None,
) -> FastAPI:
    """Create the BEB FastAPI application."""
    initialize_api_commands_table(database_path)
    app = FastAPI(title="BlackBear Edge Bridge API")

    def authenticate(
        credentials: HTTPBasicCredentials | None = Depends(SECURITY),
    ) -> str:
        if credentials is None:
            raise _auth_error()

        username_ok = secrets.compare_digest(
            credentials.username,
            config.beb_api_username,
        )
        password_ok = secrets.compare_digest(
            credentials.password,
            config.beb_api_password,
        )
        if not (username_ok and password_ok):
            raise _auth_error()

        return credentials.username

    @app.get("/health")
    def health() -> dict[str, object]:
        worker_health = _worker_health(config, odoo_worker, database_path)
        readiness_health = _readiness_health(config, readiness_monitor)
        status = "ok"
        if config.odoo_enabled and not worker_health["worker_healthy"]:
            status = "degraded"
        if readiness_health["beb_ready_enabled"] and readiness_health["beb_ready_state"] != "READY":
            status = "degraded"

        return {
            "service": "BlackBear Edge Bridge",
            "status": status,
            "mqtt_connected": _mqtt_connected(mqtt_client),
            "odoo_enabled": config.odoo_enabled,
            "api_enabled": config.beb_api_enabled,
            **worker_health,
            **readiness_health,
        }

    @app.post("/api/v1/plc/command")
    async def plc_command(
        request: Request,
        username: str = Depends(authenticate),
    ) -> JSONResponse:
        request_id = str(uuid.uuid4())
        remote_address = request.client.host if request.client else None
        idempotency_key = request.headers.get("Idempotency-Key")

        body = await request.body()
        if len(body) > config.beb_api_max_body_bytes:
            return _reject_request(
                request_id=request_id,
                idempotency_key=idempotency_key,
                username=username,
                remote_address=remote_address,
                payload="[body too large]",
                topic=config.mqtt_odoo_to_plc_topic,
                error="Request body exceeds configured maximum size",
                status_code=413,
                database_path=database_path,
            )

        raw_payload = body.decode("utf-8", errors="replace")
        try:
            parsed: object = json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            return _reject_request(
                request_id=request_id,
                idempotency_key=idempotency_key,
                username=username,
                remote_address=remote_address,
                payload=raw_payload,
                topic=config.mqtt_odoo_to_plc_topic,
                error=f"Malformed JSON: {exc.msg}",
                status_code=422,
                database_path=database_path,
            )

        try:
            command = validate_plc_command(parsed)
        except CommandValidationError as exc:
            return _reject_request(
                request_id=request_id,
                idempotency_key=idempotency_key,
                username=username,
                remote_address=remote_address,
                payload=raw_payload,
                topic=config.mqtt_odoo_to_plc_topic,
                error=str(exc),
                status_code=422,
                database_path=database_path,
            )

        plc_payload = json.dumps(command, separators=PLC_JSON_SEPARATORS)
        duplicate = _get_duplicate_response(
            idempotency_key,
            database_path,
            config.beb_api_idempotency_ttl_seconds,
        )
        if duplicate is not None:
            LOGGER.info("Duplicate API command request ignored.")
            return duplicate

        if not idempotency_key:
            LOGGER.warning(
                "No Idempotency-Key supplied for API request %s; duplicate prevention is weaker.",
                request_id,
            )

        try:
            create_api_command_record(
                request_id=request_id,
                idempotency_key=idempotency_key,
                username=username,
                remote_address=remote_address,
                payload=plc_payload,
                mqtt_topic=config.mqtt_odoo_to_plc_topic,
                database_path=database_path,
            )
        except sqlite3.IntegrityError:
            duplicate = _get_duplicate_response(
                idempotency_key,
                database_path,
                config.beb_api_idempotency_ttl_seconds,
            )
            if duplicate is not None:
                LOGGER.info("Duplicate API command request ignored.")
                return duplicate
            raise

        result: PublishResult = mqtt_client.publish_plc_command(command)
        if result.success:
            mark_api_command_published(
                request_id,
                result.rc,
                result.mid,
                database_path,
            )
            response_body = {
                "success": True,
                "status": "published",
                "request_id": request_id,
                "idempotency_key": idempotency_key,
                "topic": result.topic,
                "payload": command,
                "mqtt_mid": result.mid,
            }
            response = _stored_json_response(
                request_id,
                200,
                response_body,
                database_path,
            )
            LOGGER.info(
                "\n%s",
                format_api_success_log(
                    request_id=request_id,
                    idempotency_key=idempotency_key,
                    username=username,
                    remote_address=remote_address,
                    payload=plc_payload,
                    topic=result.topic,
                    log_payload=config.beb_api_log_request_body,
                ),
            )
            return response

        error = result.error or "MQTT broker unavailable"
        mark_api_command_failed(
            request_id,
            error,
            database_path,
            mqtt_rc=result.rc,
            mqtt_mid=result.mid,
        )
        response_body = {
            "success": False,
            "status": "failed",
            "request_id": request_id,
            "error": error,
        }
        response = _stored_json_response(request_id, 503, response_body, database_path)
        LOGGER.error(
            "\n%s",
            format_api_failure_log(
                request_id=request_id,
                status="FAILED",
                error=error,
            ),
        )
        return response

    return app


def format_api_success_log(
    *,
    request_id: str,
    idempotency_key: str | None,
    username: str,
    remote_address: str | None,
    payload: str,
    topic: str,
    log_payload: bool = True,
) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Odoo Command API Request",
            f"Request ID      : {request_id}",
            f"Idempotency Key : {idempotency_key or ''}",
            f"Username        : {username}",
            f"Remote Address  : {remote_address or ''}",
            f"Payload         : {payload if log_payload else '[disabled]'}",
            f"Topic           : {topic}",
            "Status          : PUBLISHED",
            "-" * 50,
        ]
    )


def format_api_failure_log(*, request_id: str, status: str, error: str) -> str:
    return "\n".join(
        [
            "-" * 50,
            "Odoo Command API Failed",
            f"Request ID : {request_id}",
            f"Status     : {status}",
            f"Error      : {error}",
            "-" * 50,
        ]
    )


def _auth_error() -> HTTPException:
    return HTTPException(
        status_code=401,
        detail="Invalid authentication credentials",
        headers={"WWW-Authenticate": "Basic"},
    )


def _mqtt_connected(mqtt_client: Any) -> bool:
    is_connected = getattr(mqtt_client, "is_connected", None)
    if callable(is_connected):
        return bool(is_connected())

    return False


def _worker_health(
    config: AppConfig,
    odoo_worker: Any | None,
    database_path: str | Path,
) -> dict[str, object]:
    if odoo_worker is not None:
        health_snapshot = getattr(odoo_worker, "health_snapshot", None)
        if callable(health_snapshot):
            return dict(health_snapshot(config.odoo_worker_heartbeat_seconds))

    counts = get_queue_counts(database_path)
    return {
        "worker_running": False,
        "worker_healthy": not config.odoo_enabled,
        "worker_last_heartbeat": None,
        "worker_current_message_id": None,
        "worker_current_processing_seconds": None,
        "queue_new_count": counts.get("NEW", 0),
        "queue_processing_count": counts.get("PROCESSING", 0),
        "queue_failed_count": counts.get("FAILED", 0),
        "queue_completed_count": counts.get("COMPLETED", 0),
    }


def _readiness_health(
    config: AppConfig,
    readiness_monitor: Any | None,
) -> dict[str, object]:
    if readiness_monitor is not None:
        snapshot = getattr(readiness_monitor, "snapshot", None)
        if callable(snapshot):
            value = snapshot()
            return {
                "beb_ready_enabled": bool(value.enabled),
                "beb_ready_state": value.state,
                "beb_ready_check_timeout_seconds": value.check_timeout_seconds,
                "beb_ready_last_check_at": value.last_check_at,
                "beb_ready_last_success_at": value.last_success_at,
                "beb_ready_last_failure_at": value.last_failure_at,
                "beb_ready_last_published_at": value.last_published_at,
                "beb_ready_last_error": value.last_error,
                "beb_ready_disconnect_elapsed_seconds": value.disconnect_elapsed_seconds,
                "beb_ready_recovery_elapsed_seconds": value.recovery_elapsed_seconds,
            }

    return {
        "beb_ready_enabled": config.beb_ready_enabled,
        "beb_ready_state": "UNKNOWN" if config.beb_ready_enabled else "DISABLED",
        "beb_ready_check_timeout_seconds": config.beb_ready_check_timeout_seconds,
        "beb_ready_last_check_at": None,
        "beb_ready_last_success_at": None,
        "beb_ready_last_failure_at": None,
        "beb_ready_last_published_at": None,
        "beb_ready_last_error": None,
        "beb_ready_disconnect_elapsed_seconds": None,
        "beb_ready_recovery_elapsed_seconds": None,
    }


def _get_duplicate_response(
    idempotency_key: str | None,
    database_path: str | Path,
    ttl_seconds: int,
) -> JSONResponse | None:
    if not idempotency_key:
        return None

    record = get_api_command_by_idempotency_key(idempotency_key, database_path)
    if record is None or not _record_within_ttl(record, ttl_seconds):
        return None
    if record.response_code is None or record.response_body is None:
        return JSONResponse(
            status_code=409,
            content={
                "success": False,
                "status": "duplicate_pending",
                "request_id": record.request_id,
                "error": "Original request is still being processed",
            },
        )

    return JSONResponse(
        status_code=int(record.response_code),
        content=json.loads(record.response_body),
    )


def _record_within_ttl(record: ApiCommandRecord, ttl_seconds: int) -> bool:
    if ttl_seconds <= 0:
        return False

    received_at = datetime.fromisoformat(record.received_at)
    if received_at.tzinfo is None:
        received_at = received_at.replace(tzinfo=timezone.utc)

    return received_at >= datetime.now(timezone.utc) - timedelta(seconds=ttl_seconds)


def _reject_request(
    *,
    request_id: str,
    idempotency_key: str | None,
    username: str,
    remote_address: str | None,
    payload: str,
    topic: str,
    error: str,
    status_code: int,
    database_path: str | Path,
) -> JSONResponse:
    create_api_command_record(
        request_id=request_id,
        idempotency_key=idempotency_key,
        username=username,
        remote_address=remote_address,
        payload=payload,
        mqtt_topic=topic,
        database_path=database_path,
    )
    mark_api_command_rejected(request_id, error, database_path)
    body = {
        "success": False,
        "status": "rejected",
        "request_id": request_id,
        "error": error,
    }
    response = _stored_json_response(request_id, status_code, body, database_path)
    LOGGER.error(
        "\n%s",
        format_api_failure_log(
            request_id=request_id,
            status="REJECTED",
            error=error,
        ),
    )
    return response


def _stored_json_response(
    request_id: str,
    status_code: int,
    body: dict[str, object],
    database_path: str | Path,
) -> JSONResponse:
    save_api_response(
        request_id,
        status_code,
        json.dumps(body, separators=(",", ":")),
        database_path,
    )
    return JSONResponse(status_code=status_code, content=body)
