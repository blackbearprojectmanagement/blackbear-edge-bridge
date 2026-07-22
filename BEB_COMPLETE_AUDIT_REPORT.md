# BEB Complete Audit Report

Audit date: 2026-07-22

Audit scope: local repository at `F:\BlackBear\Development\BEB`, source code, tests, Docker artifacts, documentation, and safe local command execution. BBA runtime commands requiring the Ubuntu host or a running Docker daemon could not be completed from this Windows development machine and are explicitly marked as Needs verification.

No Python source, Docker configuration, database schema, or existing documentation was modified during this audit. This report is the only added file.

## 1. Executive Summary

Overall health rating: 7/10

Production-readiness rating: 6/10

Security rating: 5/10

Reliability rating: 7/10

Maintainability rating: 8/10

Resource-efficiency rating: 7/10

BEB is well structured for the current basic bridge requirement. The repository has a clear single-entry startup path, durable SQLite queueing, Odoo timeout isolation through temporary child processes, stale PROCESSING recovery, duplicate MQTT suppression, duplicate ACK replay, debounced readiness publishing, and a Docker Compose stack with one BEB container plus one Mosquitto container.

The main blockers before BBW production are not business logic defects. They are deployment and industrial reliability risks: unauthenticated MQTT exposed on the LAN, QoS 0 / non-retained command and readiness delivery, ACK publish failures not being made durable for retry, no Docker log/resource policy, and the possibility of native and Docker deployments running at the same time unless BBA/BBW host services are explicitly checked.

Local validation results:

- `git status --short`: clean before the audit report was created.
- `git branch --show-current`: `main`.
- `git log --oneline -15`: latest commit `7012735 Expose Mosquitto on LAN for Docker deployment`.
- `git diff --stat`: clean before the audit report was created.
- `docker compose config --quiet`: PASS, with local `.env` interpolation warnings from an unquoted `$` sequence; exact token redacted.
- `docker compose config --no-interpolate`: PASS and confirmed Mosquitto publishes host port `1883`, API mapping remains `${BEB_API_BIND:-127.0.0.1}:${BEB_API_HOST_PORT:-8000}:8000`.
- `python -m compileall app`: PASS.
- `.venv\Scripts\python.exe -m pytest`: PASS, 121 tests.
- `ruff` and `mypy`: not installed in the existing environment; not run.
- Docker daemon dependent commands: NOT TESTED locally because Docker Desktop Linux engine was not reachable.
- Linux host commands `sudo ss` and `systemctl`: NOT TESTED locally because this audit ran from Windows.

## 2. Current Architecture

```text
Odoo / external caller
 |
 | HTTP Basic Auth POST /api/v1/plc/command
 v
BEB FastAPI thread inside BEB Python process
 |
 | validated JSON command, QoS 0, retain false
 v
Mosquitto container on Docker network and host LAN port 1883
 |
 v
PLC

PLC
 |
 | MQTT/PLC_TO_ODOO/topic, QoS 0
 v
Mosquitto
 |
 v
BEB MQTT network loop thread
 |
 | parse, validate, hash(topic + raw payload)
 v
SQLite mqtt_messages table in /app/data/bridge.db
 |
 v
Odoo worker thread
 |
 | claim NEW / retryable FAILED as PROCESSING
 v
temporary Odoo child process
 |
 | XML-RPC common.authenticate + object.execute_kw
 v
Odoo
 |
 | response with success/result/ACK
 v
SQLite COMPLETED + ACK stored, then ACK published to PLC topic
```

Docker architecture:

```text
Docker host
 |
 +-- beb container
 |    +-- permanent python -m app.main process
 |    +-- named volume beb-sqlite-data:/app/data
 |    +-- private Docker network beb-net
 |
 +-- beb-mosquitto container
      +-- eclipse-mosquitto:2.0
      +-- host port 1883:1883
      +-- named volumes for data and logs
      +-- read-only config bind mount
```

## 3. Runtime Process Model

Permanent application processes and threads expected from source:

| Process/service | Permanent/temporary | Purpose | Expected count |
|---|---|---|---:|
| `python -m app.main` | Permanent process | BEB application entry point | 1 per deployment |
| MQTT paho network loop | Permanent thread | Broker connection, subscribe, receive, publish | 1 |
| Odoo queue worker | Permanent thread when `ODOO_ENABLED=true` | Claims SQLite rows and submits to Odoo | 1 |
| Odoo stale watchdog | Permanent thread when `ODOO_ENABLED=true` | Recovers stale PROCESSING rows | 1 |
| Readiness monitor | Permanent thread when readiness and Odoo are enabled | Debounced Odoo readiness checks and BR publishing | 1 |
| Uvicorn server | Permanent thread when API enabled | Serves FastAPI app | 1 |
| Odoo submission child | Temporary process | Killable XML-RPC submission isolation | 0 or 1 active per worker |
| BEB health-check process | Temporary process | `python -c` HTTP health probe | 1 every 30s |
| Mosquitto process | Permanent process | MQTT broker | 1 |
| Mosquitto health-check client | Temporary process/client | `mosquitto_pub` to local broker | 1 every 30s |
| Manual `mosquitto_sub/pub` | Temporary | Human validation | 0+ only during troubleshooting |

Source evidence:

- Main startup creates one MQTT client, one Odoo worker bundle, one readiness monitor, and one API server in `app/main.py:97-124`.
- Shutdown stops readiness, API, worker, MQTT, and Odoo clients in `app/main.py:128-137`.
- Worker `start()` is guarded against double start in `app/queue_worker.py:178-183`.
- Readiness `start()` is guarded against double start in `app/readiness.py:144-152`.
- API server `start()` is guarded against double start in `app/api_server.py:45-48`.
- Uvicorn is started programmatically with no reload and no multi-worker option in `app/api_server.py:57-68`.

Needs BBA verification:

- Live `docker compose top`, `docker top beb`, `docker top beb-mosquitto`, `docker compose exec ... ps aux`, and `docker stats --no-stream` could not run locally because the Docker daemon was not reachable.

## 4. Confirmed Working Functions

Confirmed from tests and source:

- PLC messages parse for `MN` and `MP`: `app/message_parser.py:43-65`; tests in `tests/test_message_parser.py`.
- MQTT callback persists valid PLC messages before worker submission: `app/mqtt_client.py:225-260`.
- Duplicate MQTT rows are prevented by `message_hash TEXT NOT NULL UNIQUE`: `app/database.py:133-143`.
- Duplicate ACK replay exists for completed successful rows: `app/mqtt_client.py:266-344`.
- ACK replay throttling uses a count, minimum interval, and rolling window: `app/database.py:53-55`, `app/database.py:732-805`.
- Worker claims records atomically with `BEGIN IMMEDIATE`: `app/database.py:496-550`.
- Stale PROCESSING recovery exists: `app/queue_worker.py:438-457`, `app/database.py:649-688`.
- Odoo calls run in temporary child processes: `app/queue_worker.py:476-563`, `app/queue_worker.py:895-922`.
- Odoo timeout is configurable and terminal for ambiguous MN/MP timeouts: `app/queue_worker.py:332-357`.
- Readiness checks `version()` and `authenticate()`: `app/odoo_client.py:118-131`.
- Readiness down/recovery debouncing exists: `app/readiness.py:216-278`.
- API commands validate allowed payload keys: `app/command_parser.py:10-45`.
- FastAPI uses HTTP Basic for POST commands: `app/api.py:49-66`, `app/api.py:88-92`.
- Docker Compose has one BEB service and one Mosquitto service: `docker-compose.yml:1-80`.
- Mosquitto is exposed to LAN via `1883:1883`: `docker-compose.yml:42-43`.
- FastAPI is restricted to localhost by default at the host port mapping: `docker-compose.yml:21-22`.
- Tests pass: 121 passed.

## 5. Findings by Severity

### Critical

No confirmed Critical findings.

### High

ID: H-01

Title: Mosquitto is exposed on the LAN without broker authentication or TLS.

Severity: High

Affected file: `docker/mosquitto/mosquitto.conf:7-8`, `docker-compose.yml:42-43`

Evidence: Mosquitto listens on `0.0.0.0` and allows anonymous clients. Compose publishes `1883:1883`.

Operational impact: Any device on the reachable LAN segment can publish PLC command-topic messages, spoof PLC-to-Odoo production messages, subscribe to production traffic, or disrupt the bridge.

Recommended correction: Before BBW production, add compensating controls: host firewall allow-list for PLC/BEB management hosts, isolated OT VLAN, broker credentials and ACLs, and preferably MQTT over TLS if supported by the PLC environment.

Required before BBW deployment: Yes.

ID: H-02

Title: Odoo-to-PLC command delivery is broker-accept only, not PLC-confirmed.

Severity: High

Affected file: `app/mqtt_client.py:132-173`, `app/api.py:179-195`

Evidence: BEB returns API success when paho `publish()` returns success to the broker. QoS is `0` and retain is `False` in `app/mqtt_client.py:148-153`.

Operational impact: A command can be accepted by BEB/Mosquitto but missed by an offline, reconnecting, or unsubscribed PLC. The API response can say `published` even though the PLC did not apply it.

Recommended correction: Before BBW production, decide whether Odoo-to-PLC commands need PLC-level acknowledgement. If yes, add a command result/ack workflow or move appropriate topics to QoS 1 with compatible PLC session semantics.

Required before BBW deployment: Yes if production requires guaranteed command execution; otherwise document the at-most-once behavior explicitly.

ID: H-03

Title: ACK publish failure after Odoo success is not durable or retried.

Severity: High

Affected file: `app/queue_worker.py:418-429`, `app/queue_worker.py:431-436`, `app/mqtt_client.py:382-398`

Evidence: The worker stores `COMPLETED` and ACK first, then calls `_publish_ack(ack)`. `_publish_ack()` ignores the boolean return from the MQTT publisher.

Operational impact: If Odoo succeeds but ACK publish fails, SQLite records the row as completed and no retry task republishes the ACK. Duplicate PLC republish can trigger stored ACK replay, but if the PLC does not republish, the PLC may remain waiting or stale.

Recommended correction: Before BBW production, persist ACK publication state separately or add a bounded ACK outbox/retry mechanism. Keep duplicate ACK replay as a fallback, not the only recovery path.

Required before BBW deployment: Yes if PLC ACK is operationally required.

ID: H-04

Title: Native and Docker deployment can conflict if both are enabled on the same host.

Severity: High

Affected file: `README.md:669-683`, `docker-compose.yml:7`, `docker-compose.yml:40`

Evidence: README still documents a native `systemd` service, while Docker defines permanent `beb` and `beb-mosquitto` containers. If native BEB and Docker BEB both run, duplicate MQTT client IDs can disconnect each other or duplicate processing can occur.

Operational impact: Duplicate BEB instances can create MQTT client flapping, duplicate SQLite files, unexpected Odoo submissions, or port-binding conflicts with a native Mosquitto service.

Recommended correction: On BBA and BBW, verify and disable old native `beb`, `blackbear`, or `mosquitto` services before Docker becomes the reference runtime. Keep rollback instructions, but ensure only one permanent BEB runtime is enabled.

Required before BBW deployment: Yes.

### Medium

ID: M-01

Title: Readiness BR state is QoS 0, non-retained, and only published on confirmed changes.

Severity: Medium

Affected file: `app/mqtt_client.py:401-431`, `app/readiness.py:288-303`

Evidence: Readiness publishes `{"BR":value}` with QoS 0 and `retain=False`; `_confirm_state()` suppresses duplicate state publications.

Operational impact: After PLC restart, Mosquitto restart, or missed message, the PLC may not receive the current readiness state until the next confirmed state transition.

Recommended correction: Evaluate retained BR or periodic current-state publication if PLC logic depends on immediate state after restart.

Required before BBW deployment: Recommended, especially if BR drives production gating.

ID: M-02

Title: Duplicate identity uses raw topic plus raw payload only.

Severity: Medium

Affected file: `app/database.py:367-427`, `app/database.py:828-830`, `app/mqtt_client.py:243-253`

Evidence: `generate_message_hash()` hashes `f"{topic}{raw_payload}"`. Timestamp, normalized JSON, and production-cycle ID are not included.

Operational impact: Semantically identical messages with different whitespace can bypass duplicate protection. Conversely, two legitimate separate production cycles with identical topic and raw payload can be treated as duplicates forever.

Recommended correction: Confirm PLC serial/cycle uniqueness. If repeats are possible, introduce an explicit production-cycle identity or bounded duplicate window.

Required before BBW deployment: Needs business/process verification before BBW.

ID: M-03

Title: SQLite queue and API audit tables have no retention or archiving policy.

Severity: Medium

Affected file: `app/database.py:133-205`, `app/database.py:808-825`

Evidence: Tables are append-oriented. There is no cleanup job for old `COMPLETED`, `FAILED`, or `api_commands` rows.

Operational impact: Long-running deployments can accumulate unbounded database and WAL growth, eventually affecting disk, backup time, and query performance.

Recommended correction: Define a retention/archive policy for completed messages and API command audit rows. Keep production traceability requirements in mind.

Required before BBW deployment: Recommended; can be controlled operationally with monitoring for initial rollout.

ID: M-04

Title: Mosquitto health check creates a temporary MQTT publisher every 30 seconds.

Severity: Medium

Affected file: `docker-compose.yml:50-67`

Evidence: Health check runs `mosquitto_pub -h localhost -p 1883 -t beb/healthcheck -m ping` every 30 seconds.

Operational impact: Mosquitto logs can show frequent auto-generated client connections. This is expected health-check activity, not evidence of extra BEB instances. Load is low, but logs can become noisy.

Recommended correction: For production, reduce connection log verbosity, change the health check to a quieter TCP/broker check if available, or increase the interval if log noise matters.

Required before BBW deployment: No, but recommended for clean operations.

ID: M-05

Title: `.env.example` still suggests configurable MQTT host binding that Compose no longer uses.

Severity: Medium

Affected file: `.env.example:44-48`, `docker-compose.yml:42-43`, `README.md:523-532`

Evidence: `.env.example` documents `MQTT_BIND` and `MQTT_HOST_PORT`, but Compose now hard-binds `1883:1883`.

Operational impact: Operators may think MQTT remains localhost-bound by default or can be changed through `.env`, while actual Compose exposes it on all host interfaces.

Recommended correction: Update `.env.example` and README to align with `1883:1883` plus firewall/VLAN controls.

Required before BBW deployment: Yes, documentation/config clarity matters for production.

ID: M-06

Title: Local `.env` values containing unquoted `$` can trigger Compose interpolation warnings.

Severity: Medium

Affected file: `README.md:516-521`, local `.env` not displayed

Evidence: `docker compose config --quiet` succeeded but emitted unset-variable warnings derived from an unquoted `$` sequence in local `.env`; exact token redacted.

Operational impact: A secret containing `$` can be misinterpreted by Compose interpolation, causing broken credentials or confusing warnings.

Recommended correction: Keep the README guidance and ensure BBA/BBW secrets containing `$` are single-quoted in `.env`.

Required before BBW deployment: Yes, verify BBA/BBW `.env`.

ID: M-07

Title: Docker Compose lacks resource limits and Docker log rotation policy.

Severity: Medium

Affected file: `docker-compose.yml:1-80`

Evidence: Compose defines restart policies, health checks, volumes, and networks, but no CPU/memory limits or `logging` options.

Operational impact: A bug, log storm, or dependency issue can consume host disk or memory. Docker json-file logs can grow without bound under default daemon settings.

Recommended correction: Add host-level Docker daemon log rotation or Compose `logging` options and reasonable CPU/memory limits after measuring BBA resource use.

Required before BBW deployment: Recommended.

ID: M-08

Title: Key runtime dependencies are unpinned.

Severity: Medium

Affected file: `requirements.txt:3`, `requirements.txt:13`

Evidence: `fastapi` and `uvicorn` are listed without version pins. Several other packages are pinned.

Operational impact: Rebuilds can pull newer incompatible versions and change FastAPI/Uvicorn behavior without a code change.

Recommended correction: Pin application runtime dependencies and periodically update through tested dependency refreshes.

Required before BBW deployment: Recommended.

ID: M-09

Title: Health endpoint is unauthenticated and exposes operational state.

Severity: Medium

Affected file: `app/api.py:68-86`

Evidence: `/health` has no authentication dependency and reports MQTT connection state, worker state, readiness state, and queue counts.

Operational impact: If the API is accidentally exposed beyond localhost/tunnel controls, an unauthenticated party can learn operational status.

Recommended correction: Keep host binding to localhost and restrict tunnel/reverse-proxy access. Consider a minimal unauthenticated liveness endpoint plus authenticated detailed health.

Required before BBW deployment: No if API remains local; yes if exposed through a broader network path.

### Low

ID: L-01

Title: API payload decode uses UTF-8 replacement rather than rejecting invalid UTF-8.

Severity: Low

Affected file: `app/api.py:97-125`

Evidence: Request body is decoded with `errors="replace"` before JSON parsing.

Operational impact: Invalid byte sequences inside JSON strings may become replacement characters. Command validation then accepts any non-empty print-job value for `messt01/02/03`.

Recommended correction: Reject invalid UTF-8 explicitly for command endpoints.

Required before BBW deployment: No.

ID: L-02

Title: No API rate limiting or lockout.

Severity: Low

Affected file: `app/api.py:49-66`, `app/api.py:88-239`

Evidence: API uses HTTP Basic but has no request-rate control.

Operational impact: If exposed beyond localhost/proxy controls, repeated invalid requests can consume logs, SQLite writes, and CPU.

Recommended correction: Apply rate limits at reverse proxy/tunnel layer or add application-level limits if exposure grows.

Required before BBW deployment: No if API remains local and proxy controlled.

ID: L-03

Title: Mosquitto read-only config mount can cause harmless container chown warnings.

Severity: Low

Affected file: `docker-compose.yml:45`

Evidence: The config file is mounted read-only into `/mosquitto/config/mosquitto.conf`.

Operational impact: Eclipse Mosquitto images may attempt ownership changes and log `chown: /mosquitto/config/mosquitto.conf: Read-only file system`. This is usually harmless when Mosquitto still reads the config and starts.

Recommended correction: Treat as harmless if container is healthy. If noisy, use a writable config volume or image-specific mount pattern.

Required before BBW deployment: No.

ID: L-04

Title: Local audit-created pytest cache temp directory is access-denied to PowerShell.

Severity: Low

Affected file: local generated directory `pytest-cache-files-*`

Evidence: Pytest passed but warned it could not create cache path; subsequent PowerShell file walks reported access denied for the generated temp directory.

Operational impact: Local development housekeeping only. No source or tracked file impact was observed before report creation.

Recommended correction: Remove or repair permissions outside this audit if desired.

Required before BBW deployment: No.

ID: L-05

Title: Runtime command tools `ruff` and `mypy` are absent from the existing environment.

Severity: Low

Affected file: no project config found

Evidence: `ruff` and `mypy` were not on PATH and not importable through the existing virtualenv.

Operational impact: Static lint/type checks are not currently part of the reproducible audit path.

Recommended correction: Add dev-only tooling configuration if desired.

Required before BBW deployment: No.

### Informational

ID: I-01

Title: Docker Compose structure validates.

Severity: Informational

Affected file: `docker-compose.yml:1-80`

Evidence: `docker compose config --quiet` passed.

Operational impact: Compose syntax is valid.

Recommended correction: None.

Required before BBW deployment: No.

ID: I-02

Title: Local Docker runtime commands could not inspect containers.

Severity: Informational

Affected file: runtime environment

Evidence: Docker daemon connection failed at the Windows Docker Desktop Linux engine pipe.

Operational impact: Live BBA process count, container logs, image list, Docker stats, and container inspect output were not available from this machine.

Recommended correction: Run the required runtime commands directly on BBA.

Required before BBW deployment: Yes, as an operational verification step.

ID: I-03

Title: Tests pass.

Severity: Informational

Affected file: `tests/*`

Evidence: `.venv\Scripts\python.exe -m pytest` collected and passed 121 tests.

Operational impact: Good regression confidence for parser, API, DB, MQTT logging/ACK, Odoo client, queue worker, and readiness logic.

Recommended correction: Add the missing high-value tests listed in section 16.

Required before BBW deployment: No.

ID: I-04

Title: Mosquitto LAN exposure correction is present in Git.

Severity: Informational

Affected file: `docker-compose.yml:42-43`

Evidence: The latest commit is `7012735 Expose Mosquitto on LAN for Docker deployment`; Compose maps `1883:1883`.

Operational impact: PLCs connecting to the Ubuntu LAN IP on port 1883 should no longer be blocked by localhost-only Docker port binding.

Recommended correction: Verify from PLC/BBA with `docker compose ps` and PLC subscription behavior.

Required before BBW deployment: Operational verification required.

## 6. MQTT Audit

Configuration:

- Broker host defaults to `localhost`; Docker overrides to `mosquitto`: `app/config.py:82-83`, `docker-compose.yml:14-16`.
- Main PLC-to-BEB topic default: `MQTT/PLC_TO_ODOO/topic`: `app/config.py:85-87`.
- Main BEB-to-PLC topic default: `MQTT/ODOO_TO_PLC/topic`: `app/config.py:88-90`.
- Readiness topic default: `MQTT/ODOO_TO_PLC/topic`: `app/config.py:139-141`.
- MQTT protocol: MQTT v3.1.1: `app/mqtt_client.py:57-61`.
- QoS: constant `QOS = 0`: `app/mqtt_client.py:25-27`.
- Retain: command, ACK, and readiness publishes all use `retain=False`: `app/mqtt_client.py:148-153`, `app/mqtt_client.py:382-388`, `app/mqtt_client.py:401-405`.
- Reconnect delay: 1 to 30 seconds: `app/mqtt_client.py:65`.
- Resubscribe happens in `on_connect`: `app/mqtt_client.py:183-210`.
- Callback error handling rejects malformed PLC messages and logs warnings: `app/mqtt_client.py:225-241`.

Topic consistency:

No wildcard subscriptions were found. Topics are consistently configured through environment variables for main PLC/Odoo paths. The `beb/healthcheck` topic is Docker-health-only: `docker-compose.yml:59-60`.

QoS/retain assessment:

- Production PLC-to-Odoo messages: QoS 0 is simple and low overhead but at-most-once. SQLite protects only messages that BEB receives.
- Odoo-to-PLC print/model commands: QoS 0 and retain false means no delivery if PLC is offline or not subscribed. This is a business decision, not guaranteed delivery.
- ACK messages: QoS 0 and retain false is acceptable only if PLC republish/duplicate ACK replay is an accepted recovery path.
- Readiness BR: QoS 0 and retain false can leave PLC with stale readiness after missed messages or broker/PLC restart.

Offline PLC behavior:

If BEB publishes while the PLC is offline, Mosquitto does not retain the command and QoS 0 does not queue it for later delivery.

Broker restart behavior:

BEB should reconnect and resubscribe automatically through paho loop/reconnect behavior and `on_connect` subscription. Live BBA verification still needed.

## 7. Odoo Integration Audit

Path:

1. MQTT callback saves valid PLC message to SQLite: `app/mqtt_client.py:243-251`.
2. Worker atomically claims NEW/retryable FAILED rows: `app/database.py:496-550`.
3. Worker reconstructs the original raw JSON payload for Odoo: `app/queue_worker.py:659-679`.
4. Worker runs one child process per submission: `app/queue_worker.py:476-563`.
5. Child builds Odoo client from serializable settings and calls `submit_print_data`: `app/queue_worker.py:895-922`.
6. Odoo client authenticates and calls `execute_kw`: `app/odoo_client.py:87-107`, `app/odoo_client.py:148-180`.
7. Success with valid ACK completes the row and publishes ACK: `app/queue_worker.py:404-429`.

Timeout behavior:

- XML-RPC transport timeout is applied in `_TimeoutTransport` and `_TimeoutSafeTransport`: `app/odoo_client.py:226-254`.
- Worker hard timeout joins the child for `self._submission_timeout`: `app/queue_worker.py:497`.
- Timed-out child is terminated/killed and the row is marked terminal FAILED, retryable false: `app/queue_worker.py:332-357`, `app/queue_worker.py:508-523`.

Can Odoo execute more than once for one PLC transaction?

- For a single saved row, normal worker logic claims and submits once at a time.
- Retryable failures can re-submit the same row later.
- Ambiguous timeouts are intentionally not retried.
- If two BEB instances point at the same SQLite DB, `BEGIN IMMEDIATE` protects claim race. If two BEB instances use separate DBs, duplicate Odoo execution is possible.
- If raw payload formatting differs, duplicate hash differs and Odoo can be called again.

Conditions:

- Duplicate Odoo update: separate BEB DBs, raw payload formatting changes, retryable failure after Odoo actually completed but returned transport error, or manual duplicate with changed topic/payload.
- Lost Odoo update: message lost before BEB receives it due QoS 0, SQLite unavailable, or malformed/unsupported payload rejected.
- Ambiguous final state: Odoo timeout, crash during child call, crash after Odoo success before SQLite completion.
- Stuck PROCESSING: crash can leave PROCESSING until watchdog/stale recovery runs.
- ACK without confirmed Odoo success: no confirmed path found; ACK extraction requires success true and non-empty ACK.
- Odoo success without ACK publication: confirmed risk H-03.

## 8. SQLite and Transaction Audit

Database location:

- Default local path: `data/bridge.db`: `app/database.py:15`, `app/config.py:92`.
- Docker path: `/app/data/bridge.db`: `docker-compose.yml:17`.
- Docker persistence: `beb-sqlite-data:/app/data`: `docker-compose.yml:23-24`.

Connection behavior:

- New connection per operation, no shared global connection: `app/database.py:838-857`.
- SQLite timeout: 30 seconds: `app/database.py:839`.
- WAL mode: `PRAGMA journal_mode=WAL`: `app/database.py:841`.
- Synchronous FULL: `app/database.py:842`.
- Transactions use `BEGIN` or `BEGIN IMMEDIATE`: `app/database.py:509`, `app/database.py:744`, `app/database.py:850`.

Current schema:

`mqtt_messages`:

- `id`: primary key.
- `received_at`: UTC timestamp.
- `topic`: MQTT topic.
- `message_type`: `MN` or `MP`.
- `table_no`: `T01`, `T02`, or `T03`.
- `model`: parsed model number.
- `serial`: parsed serial.
- `raw_payload`: original decoded payload.
- `message_hash`: unique topic + raw payload hash.
- `status`: NEW, PROCESSING, COMPLETED, FAILED.
- `retry_count`: retry counter.
- `processed_at`, `last_error`, `odoo_response`, `last_attempt_at`, `completed_at`: processing audit fields.
- `ack`, `customer_id`, `customer_name`, `operator_id`, `operator_name`, `batch_number`: Odoo result metadata.
- `ack_replay_count`, `last_ack_replayed_at`: duplicate ACK replay throttling.

`api_commands`:

- `id`: primary key.
- `request_id`: unique UUID per API request.
- `idempotency_key`: unique optional command idempotency key.
- `received_at`, `username`, `remote_address`, `payload`, `payload_hash`, `mqtt_topic`: audit fields.
- `status`: RECEIVED, PUBLISHED, FAILED, DUPLICATE, REJECTED.
- `mqtt_rc`, `mqtt_mid`, `published_at`, `response_code`, `response_body`, `last_error`: publish/response fields.

Allowed and actual transitions:

- `NEW -> PROCESSING -> COMPLETED`
- `NEW -> PROCESSING -> FAILED`
- `FAILED -> PROCESSING -> COMPLETED`
- `FAILED -> PROCESSING -> FAILED`
- `PROCESSING -> FAILED` by watchdog recovery
- Legacy direct updates through `update_status()` can set any valid status for a row: `app/database.py:468-493`.

Race assessment:

- Duplicate insertion is protected by unique hash and IntegrityError fallback: `app/database.py:383-420`.
- Two workers sharing one DB should not claim the same rows because `claim_pending_messages()` uses `BEGIN IMMEDIATE`.
- No SQLite connections are shared with child processes for Odoo submission. Child processes write result pickle files, not DB rows.

## 9. Readiness Audit

Startup:

- Created in `app/main.py:111`.
- Started after MQTT loop start and before API start: `app/main.py:121-124`.
- Disabled unless both `BEB_READY_ENABLED` and `ODOO_ENABLED` are true: `app/main.py:61`.

Check behavior:

- Uses separate Odoo client with readiness timeout: `app/main.py:75-83`.
- Calls `common.version()` and `authenticate()`: `app/odoo_client.py:118-131`.
- Default check interval: 1 second: `app/config.py:127-129`.
- Default check timeout: 3 seconds: `app/config.py:130-132`.
- Default down debounce: 5 seconds: `app/config.py:133-135`.
- Default recovery debounce: 10 seconds: `app/config.py:136-138`.

State behavior:

- Initial enabled state is UNKNOWN: `app/readiness.py:45`.
- First failure confirms NOT_READY immediately with BR=0: `app/readiness.py:258-260`.
- Success from UNKNOWN/NOT_READY requires recovery debounce before READY: `app/readiness.py:235-246`.
- Failure from READY requires down debounce before NOT_READY: `app/readiness.py:266-278`.
- Duplicate confirmed states are suppressed: `app/readiness.py:288-290`.

Load:

- With default 1-second interval, readiness performs frequent `version()` and authentication checks. This is acceptable for a small system but can create Odoo logins and XML-RPC traffic. Consider increasing interval if Odoo logs/noise matter.

## 10. FastAPI Audit

Endpoints:

| Method | Path | Purpose | Auth | Side effects |
|---|---|---|---|---|
| GET | `/health` | Health/status snapshot | None | Reads SQLite counts and runtime states |
| POST | `/api/v1/plc/command` | Odoo-to-PLC command publish | HTTP Basic | Writes `api_commands`, publishes MQTT |

FastAPI behavior:

- App is created with no CORS middleware: `app/api.py:38-47`.
- HTTP Basic uses constant-time compare: `app/api.py:49-66`.
- Body size limit defaults to 16 KiB: `app/api.py:97-109`, `app/config.py:42`.
- JSON payload is validated by command parser: `app/api.py:111-140`, `app/command_parser.py:16-45`.
- Idempotency key duplicate responses are stored/replayed: `app/api.py:142-150`, `app/api.py:358-394`.
- API bind inside container is `0.0.0.0`, but host mapping defaults to localhost: `docker-compose.yml:18-22`.

Exposure risk:

FastAPI remains restricted by Compose default host bind. It can be exposed if `BEB_API_BIND` is set to `0.0.0.0` or a LAN IP, or if a tunnel/reverse proxy forwards it.

## 11. Docker Audit

Dockerfile:

- Base image: `python:3.12-slim`: `Dockerfile:1`.
- Non-root user: `beb`: `Dockerfile:8`, `Dockerfile:19`.
- Working dir: `/app`: `Dockerfile:6`.
- Requirements install: `Dockerfile:10-12`.
- Application copy only: `Dockerfile:14`.
- Data/log directories created and owned by `beb`: `Dockerfile:16-17`.
- Exposes port 8000: `Dockerfile:21`.
- Runs `python -m app.main`: `Dockerfile:23`.

Compose:

- One BEB service: `docker-compose.yml:2-36`.
- One Mosquitto service: `docker-compose.yml:38-67`.
- `depends_on` waits for Mosquitto health: `docker-compose.yml:9-11`.
- Restart policies are `unless-stopped`: `docker-compose.yml:8`, `docker-compose.yml:41`.
- Persistent SQLite volume: `docker-compose.yml:23-24`, `docker-compose.yml:69-71`.
- Persistent Mosquitto data/log volumes: `docker-compose.yml:44-47`, `docker-compose.yml:72-75`.
- Shared bridge network: `docker-compose.yml:25-26`, `docker-compose.yml:77-80`.
- BEB health check calls `/health`: `docker-compose.yml:27-36`.
- Mosquitto health check publishes to `beb/healthcheck`: `docker-compose.yml:50-67`.

`depends_on` caveat:

It gates BEB startup on Mosquitto service health, but does not guarantee PLC readiness, Odoo readiness, or successful future broker availability.

## 12. Mosquitto Audit

Image:

- `eclipse-mosquitto:2.0`: `docker-compose.yml:39`.

Configuration:

- Persistence enabled: `docker/mosquitto/mosquitto.conf:1`.
- Persistence location: `/mosquitto/data/`: `docker/mosquitto/mosquitto.conf:2`.
- Logs to stdout with timestamps: `docker/mosquitto/mosquitto.conf:4-5`.
- Listener on all interfaces inside container: `docker/mosquitto/mosquitto.conf:7`.
- Anonymous access enabled: `docker/mosquitto/mosquitto.conf:8`.

Logging:

No `log_type all` or `connection_messages true` settings are present in the tracked Mosquitto config. Default Mosquitto logging may still show connection/disconnection notices depending image defaults.

Production logging recommendation:

Use a configuration that keeps errors, warnings, and important notices while reducing repetitive client connection logs. Example policy, not implemented:

```text
log_dest stdout
log_timestamp true
log_type error
log_type warning
log_type notice
connection_messages false
```

Read-only config warning:

`chown: /mosquitto/config/mosquitto.conf: Read-only file system` is generally harmless if Mosquitto starts and reads the config. It indicates the image attempted ownership adjustment on a read-only bind mount. It is not evidence of failed config loading by itself.

## 13. Logging and Resource Audit

Application logs include:

- MQTT broker connection and subscription events: `app/mqtt_client.py:67-78`, `app/mqtt_client.py:183-223`.
- Full raw PLC payload and parsed model/serial/table: `app/mqtt_client.py:347-379`.
- Duplicate ACK replay decisions: `app/mqtt_client.py:266-344`.
- Odoo submission start, success, failure, timeout, and business failure: `app/queue_worker.py:740-867`.
- API request success/failure with payload optional logging: `app/api.py:202-213`, `app/api.py:244-280`.
- Readiness raw checks and debounce state: `app/readiness.py:187-192`, `app/readiness.py:216-325`.

Sensitive information:

- Odoo password is not logged in source.
- API Authorization header is not logged.
- Odoo responses are logged and stored; confirm they never contain sensitive customer data beyond expected production metadata.

Production log policy:

- BEB: INFO in BBA testing, INFO/WARNING in BBW, avoid DEBUG except short troubleshooting windows.
- Mosquitto: errors/warnings/notices, suppress repetitive health-check connection messages if noisy.
- Docker: configure json-file rotation or host logrotate.
- SQLite: use DB history as transaction audit; add retention/backup policy.

## 14. Security Audit

Security posture:

- API command route uses HTTP Basic and constant-time credential comparison.
- MQTT currently has no authentication, authorization, or TLS.
- SQL access uses parameterized queries except controlled SQL fragments for placeholders/column definitions.
- No shell invocation exists in runtime code.
- XML-RPC target URL/model/method are environment-configured.
- Docker BEB runs as non-root.
- Mosquitto container behavior/user could not be live-inspected locally.

Primary risks:

- Anonymous MQTT LAN access is the largest confirmed risk.
- API can be safe if bound locally and tunnel/proxy controlled.
- Secrets are kept in `.env`, ignored by Git, and `.env.example` contains blank password fields.
- Docker Compose full config can display resolved environment; avoid posting it without redaction.

## 15. Reliability and Failure-Recovery Audit

Scenario assessment:

| Scenario | Current behavior |
|---|---|
| PLC disconnect | BEB can still publish to broker; QoS 0 messages may be missed |
| PLC restart | No retained commands/readiness; current BR may not replay |
| Mosquitto restart | BEB paho loop should reconnect and resubscribe; needs live validation |
| BEB restart | Docker restart policy restarts container unless stopped |
| Ubuntu reboot | Compose restart policy supports restart if Docker starts containers; host verification needed |
| Odoo outage | Worker logs failure and retryable rows can retry |
| Odoo slow response | Configurable timeout; ambiguous timeout terminal FAILED |
| Duplicate PLC publish | Duplicate hash suppresses insert and can replay ACK if completed |
| Malformed MQTT JSON | Rejected and logged; not persisted |
| Invalid model number | Parser validates serial/table, not model domain |
| SQLite locked | 30-second timeout; exception path logs in caller loops |
| SQLite corruption | No built-in repair; rely on backup/restore |
| Disk full | No explicit handling beyond exceptions |
| Docker volume missing | SQLite recreated empty; restore procedure needed |
| Wrong env vars | Config validates int/bool types; semantic mistakes still possible |
| Crash during Odoo call | PROCESSING recovered later; Odoo final state may be ambiguous |
| Crash after Odoo success before ACK | Row may be incomplete or completed without ACK depending timing |
| Crash after ACK publish before DB update | Current order marks DB completed before ACK publish, so this exact order is avoided |

Single points of failure:

- One Mosquitto broker.
- One SQLite DB volume.
- One BEB process.
- Odoo endpoint availability.
- Host disk and Docker volume health.

## 16. Test Coverage Audit

Existing tests cover:

- Message parser: `tests/test_message_parser.py`.
- Database duplicate/state/recovery: `tests/test_database.py`.
- API endpoints/idempotency/validation/health: `tests/test_beb_api.py`.
- Odoo client auth/execute/timeout/readiness: `tests/test_odoo_client.py`.
- Queue worker success/failure/timeout/stale recovery/ACK: `tests/test_queue_worker.py`.
- MQTT logging and duplicate ACK replay: `tests/test_mqtt_logging.py`, `tests/test_mqtt_duplicate_ack_replay.py`.
- Readiness debounce: `tests/test_readiness.py`.
- Main factory behavior: `tests/test_main.py`.

Missing high-value tests:

- MQTT reconnect/resubscribe integration test with a real broker.
- Docker Compose startup health integration test.
- Mosquitto persistence across container recreation.
- SQLite Docker volume persistence across `up -d --build`.
- ACK publish failure durability/retry behavior.
- PLC offline/missed command behavior and explicit expected contract.
- Readiness behavior after Mosquitto restart and PLC restart.
- Duplicate false-positive case for repeated identical payload in a new production cycle.
- API exposure/rate-limit/proxy behavior if deployed beyond localhost.

## 17. Deployment Consistency Audit

Git:

- Branch: `main`.
- Recent relevant commits present:
  - `7012735 Expose Mosquitto on LAN for Docker deployment`
  - `d904f4f Add Docker deployment artifacts`
  - `4337043 Add debounced BEB readiness signal`
  - `f6d0a0b Suppress retries after ambiguous Odoo timeouts`
- Working tree was clean before this report was added.

Tracked deployment files:

- `Dockerfile`
- `docker-compose.yml`
- `.dockerignore`
- `.env.example`
- `docker/mosquitto/mosquitto.conf`
- README Docker/native deployment sections

Ignored local files:

- `.env` ignored by `.gitignore:2`.
- `data/*.db` ignored by `.gitignore:5`.
- pyc caches ignored by `.gitignore:3-4`.

BBA/BBW consistency:

Source now supports BBA Docker as the reference deployment. BBW can be deployed from the same repository, but only after host-level checks confirm native BEB/Mosquitto services are not conflicting and the production `.env` is correct.

Rollback:

README contains backup/restore and update commands, but explicit rollback-to-previous-Git-commit instructions are not present.

## 18. Essential vs Optional Components

| Component | Essential now | Optional now | Reason |
|---|---:|---:|---|
| BEB Python process | Yes | No | Core bridge logic |
| Mosquitto broker | Yes | No | PLC/BEB MQTT transport |
| SQLite queue | Yes | No | Durable PLC-to-Odoo transaction state |
| Odoo XML-RPC client | Yes when Odoo enabled | No | Required for production updates |
| Odoo child subprocess isolation | Yes | No | Prevents hung XML-RPC from blocking worker indefinitely |
| FastAPI command endpoint | Yes for Odoo-to-PLC push | No | External command path |
| Readiness BR monitor | Yes if PLC uses BR | Optional otherwise | Operational state signal |
| Docker Compose | Yes for reference deployment | No | Standardized BBA/BBW deployment |
| Native systemd deployment | No | Yes | Legacy/rollback path; can conflict if enabled |
| Docker health checks | Yes | No | Restart/visibility support |
| Mosquitto persistent data volume | Recommended | Optional for QoS 0 only | Supports broker persistence if config evolves |
| Test scripts | No | Yes | Manual validation aids |
| Odoo sample/reference folders | No | Yes | Historical reference only |

## 19. Process and Resource Summary

| Process/service | Permanent/temporary | Purpose | Expected count |
|---|---|---|---:|
| BEB container | Permanent | App runtime | 1 |
| `python -m app.main` | Permanent | Main app | 1 |
| MQTT loop thread | Permanent | MQTT IO | 1 |
| Odoo worker thread | Permanent | Queue processing | 0 or 1 |
| Odoo watchdog thread | Permanent | Stale recovery | 0 or 1 |
| Readiness thread | Permanent | Odoo readiness BR | 0 or 1 |
| Uvicorn server thread | Permanent | FastAPI | 0 or 1 |
| Odoo child process | Temporary | One XML-RPC call | 0 or 1 active |
| BEB health check | Temporary | HTTP health | 1 per 30s |
| Mosquitto container | Permanent | MQTT broker | 1 |
| Mosquitto health-check publisher | Temporary | Broker health | 1 per 30s |
| Manual MQTT clients | Temporary | Diagnostics | 0+ |

## 20. BBA Validation Status

Repository/Compose validation: PASS

Local tests: PASS

Docker runtime inspection from this machine: NOT TESTED

BBA live process count: NOT TESTED

BBA port binding after latest pull/recreate: Needs verification with `docker compose ps` on BBA.

PLC receives BEB command after `1883:1883`: Needs verification on BBA/PLC.

BBA ready for continued testing: PARTIAL. Source and Compose are ready; direct BBA runtime checks still need to be run on the Ubuntu laptop.

## 21. BBW Production Readiness

Mandatory corrections:

- Add MQTT compensating controls for LAN exposure: firewall/VLAN/ACL/auth as feasible.
- Verify no native BEB or native Mosquitto service conflicts with Docker on BBW.
- Resolve `.env.example` / README mismatch around `MQTT_BIND`.
- Verify production `.env` quotes secrets containing `$`.
- Decide and document whether Odoo-to-PLC commands require PLC-confirmed delivery.
- Address ACK publish durability if PLC ACK is operationally required.

Recommended corrections:

- Configure Docker log rotation/resource limits.
- Pin FastAPI/Uvicorn versions.
- Add SQLite retention/archive policy.
- Add BBA runtime verification checklist and rollback instructions.
- Tune Mosquitto logging to reduce health-check noise.

Later improvements:

- MQTT auth/ACL/TLS if PLC and operational environment permit.
- More integration tests with a real Mosquitto container.
- Separate liveness and readiness endpoints.
- Optional command-result workflow for API callers.

## 22. Prioritized Action Plan

P0 - immediate:

- Run BBA live verification commands directly on Ubuntu.
- Confirm `docker compose ps` shows `0.0.0.0:1883->1883/tcp` or equivalent and `127.0.0.1:8000->8000/tcp`.
- Confirm only one BEB MQTT client and no native BEB service.
- Confirm PLC receives a safe validation command through Mosquitto after BBA pull/recreate.

P1 - before BBW deployment:

- Apply MQTT LAN security controls.
- Update `.env.example`/README to match hard LAN exposure.
- Verify BBW `.env` quoting and secret handling.
- Decide command/ACK delivery guarantees.
- Add Docker log rotation/resource guardrails.
- Confirm native Mosquitto cannot bind/conflict with Docker port 1883.

P2 - after deployment:

- Add retention/archive for SQLite.
- Add real-broker MQTT reconnect and persistence tests.
- Tune readiness interval if Odoo login/log volume is high.
- Pin all runtime dependencies.

P3 - platform expansion:

- MQTT ACL/TLS hardening if supported.
- Command lifecycle state machine for Odoo-to-PLC delivery confirmation.
- Centralized observability/dashboard.
- HA/backup strategy beyond single SQLite volume.

## 23. Final Verdict

Final verdict: Ready after mandatory corrections.

Direct answers:

1. Is BEB currently running more permanent processes or instances than required?
   Source and Compose show no. Live BBA verification is still needed because Docker daemon access was unavailable locally.

2. Are the many Mosquitto log entries caused by extra BEB instances or health checks?
   Source/Compose indicate the recurring auto-generated MQTT clients are very likely Docker Mosquitto health-check clients, because `mosquitto_pub` runs every 30 seconds. Do not assume extra BEB instances from those logs alone.

3. Is the current architecture appropriate for the basic Odoo/PLC requirement?
   Yes, for controlled BBA testing and basic at-most-once MQTT behavior. For BBW production, security and delivery expectations must be tightened or explicitly accepted.

4. Which components are essential?
   BEB app, Mosquitto, SQLite, Odoo XML-RPC worker when Odoo is enabled, FastAPI command endpoint, Docker Compose for deployment, and readiness if PLC logic uses BR.

5. Which components are optional?
   Native systemd deployment, sample XML-RPC scripts, manual test scripts, and future dashboard/platform tooling.

6. Which components appear excessive?
   None of the core runtime mechanisms are excessive for industrial reliability. The 1-second readiness check may be more frequent than necessary once stable.

7. Is the BBA Docker deployment ready for prolonged testing?
   Yes, after running the live BBA verification commands and confirming PLC receive behavior after the latest `1883:1883` fix.

8. Is it ready for BBW production deployment?
   Not yet. It is ready after mandatory corrections and direct BBW host validation.

9. What must be corrected before BBW deployment?
   MQTT LAN security, native/Docker conflict checks, env/doc mismatch, secret quoting verification, command/ACK delivery decision, and Docker operational guardrails.

10. What can safely wait until later platform expansion?
    Dashboarding, HA, centralized observability, expanded APIs, and full MQTT TLS/ACL rollout if current PLC constraints require a phased approach.

Concise audit summary:

- Total findings: 22
- Critical: 0
- High: 4
- Medium: 9
- Low: 5
- Informational: 4
- Multiple BEB instances found: Not found from source/Compose; live BBA verification required.
- Excessive health-check logging found: Source confirms likely repetitive health-check clients every 30 seconds; live log volume not verified locally.
- BBA ready for continued testing: PARTIAL/PASS after direct BBA runtime checks.
- BBW deployment recommended: Not until mandatory corrections are complete.
- Report path: `BEB_COMPLETE_AUDIT_REPORT.md`
