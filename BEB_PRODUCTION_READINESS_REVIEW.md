# BEB Production Readiness Review

Review date: 2026-07-22

Scope: validation and challenge of `BEB_COMPLETE_AUDIT_REPORT.md`, focused only on the original Critical and High severity findings. This is not a new repository audit.

Inputs used:

- Existing audit report: `BEB_COMPLETE_AUDIT_REPORT.md`
- Source evidence cited in that report
- Verified BBA Docker runtime facts supplied by the operator

No code, configuration, Docker files, database schema, or existing documentation were modified. No commit or push was performed.

## Executive Summary

The previous audit had no Critical findings and four High findings:

- H-01: unauthenticated Mosquitto LAN exposure
- H-02: Odoo-to-PLC command delivery is broker-accept only
- H-03: ACK publish failure after Odoo success is not durably retried
- H-04: native and Docker deployment can conflict if both are enabled

After challenging those findings against the verified BBA runtime facts, none remains a confirmed production blocker for the current BEB role.

The runtime evidence materially changes the readiness decision:

- Docker Mosquitto LAN exposure is verified working.
- Port 1883 is reachable.
- PLC client `FX5U_TEST` connects successfully.
- MQTT PINGREQ/PINGRESP is working.
- BEB publishes the correct payload.
- A subscriber receives the correct payload.
- The PLC finally receives the payload.
- D340 updates.
- The print job starts.
- Docker restart works.
- Ubuntu reboot restart works.
- SQLite persistence is verified.
- Mosquitto persistence is verified.
- BEB reconnect is verified.

Those observations directly refute the earlier uncertainty around BBA runtime readiness. The remaining issues are production risks and operational controls, not code blockers.

Final readiness decision: READY FOR CONTROLLED PRODUCTION DEPLOYMENT.

This does not mean "fully production ready." It means BEB is fit for BBW deployment in the current industrial edge role, provided the deployment remains controlled: trusted LAN, one Docker stack, known PLC/Odoo endpoints, local FastAPI binding, and operational monitoring.

## Audit Findings Validation

### Critical Findings

The original audit reported no confirmed Critical findings.

| Finding ID | Title | Severity | Affected files | Evidence | Current implementation | Why it was marked High/Critical |
|---|---|---|---|---|---|---|
| None | No Critical findings | Critical | Not applicable | `BEB_COMPLETE_AUDIT_REPORT.md:158-160` | Not applicable | Not applicable |

### High Findings

| Finding ID | Title | Severity | Affected files | Evidence | Current implementation | Why it was marked High/Critical |
|---|---|---|---|---|---|---|
| H-01 | Mosquitto is exposed on the LAN without broker authentication or TLS | High | `docker/mosquitto/mosquitto.conf:7-8`, `docker-compose.yml:42-43` | Mosquitto listens on `0.0.0.0`, `allow_anonymous true`, and Compose publishes `1883:1883` | Mosquitto is intentionally exposed on the LAN so the PLC can connect through the Ubuntu host IP | Marked High because any device on the reachable LAN could publish/subscribe unless network controls restrict access |
| H-02 | Odoo-to-PLC command delivery is broker-accept only, not PLC-confirmed | High | `app/mqtt_client.py:132-173`, `app/api.py:179-195` | BEB returns API success when MQTT publish to broker succeeds; publish uses QoS 0 and retain false | BEB implements at-most-once MQTT command publish, not a PLC command-completion protocol | Marked High because an offline/unsubscribed PLC could miss a command while BEB reports broker publish success |
| H-03 | ACK publish failure after Odoo success is not durable or retried | High | `app/queue_worker.py:418-429`, `app/queue_worker.py:431-436`, `app/mqtt_client.py:382-398` | Worker stores row as COMPLETED and ACK, then publishes ACK; `_publish_ack()` does not act on publisher return value | Odoo success is durable in SQLite; ACK is best-effort MQTT publish, with duplicate ACK replay if PLC republishes duplicate input | Marked High because a broker/network failure after Odoo success could leave Odoo complete while PLC misses ACK |
| H-04 | Native and Docker deployment can conflict if both are enabled | High | `README.md:669-683`, `docker-compose.yml:7`, `docker-compose.yml:40` | README documents native systemd service; Compose defines Docker `beb` and `beb-mosquitto` containers | Repository contains both legacy/native instructions and Docker deployment; Compose itself defines only one BEB service | Marked High because simultaneous native and Docker BEB could create duplicate MQTT clients or duplicate processing |

## Challenge of Each Finding

### H-01: Mosquitto LAN Exposure Without Auth/TLS

1. Is the finding actually reproducible?

Yes for exposure, not for failure. Source and runtime both confirm Mosquitto is exposed on LAN. The verified facts say port 1883 is reachable and PLC client `FX5U_TEST` connects successfully.

2. Is it proven from source code?

Yes. `docker/mosquitto/mosquitto.conf:7-8` sets `listener 1883 0.0.0.0` and `allow_anonymous true`. `docker-compose.yml:42-43` maps `1883:1883`.

3. Is it proven from runtime?

Yes for intended LAN exposure and PLC connectivity. The supplied verified facts confirm port 1883 is reachable and the PLC connects.

4. Is it based only on static analysis?

No. Static evidence is confirmed by BBA runtime evidence.

5. Could it be a false positive?

No as a security exposure. It is real. It is a false positive only if interpreted as a functional production blocker, because this exposure is required for the PLC to connect in the current architecture.

6. Could it already be mitigated elsewhere?

Possibly by physical LAN trust, switch isolation, firewall rules, or OT network controls, but those controls are not proven by repository evidence. The provided runtime facts prove function, not network access policy.

7. Does another module already prevent this problem?

No BEB module authenticates MQTT clients or validates broker-level publisher identity. BEB validates payload shape after receipt, but Mosquitto itself allows anonymous publish/subscribe.

Challenge result:

This is a real production security risk, but not a confirmed production blocker for a controlled LAN deployment. It can be accepted with operational controls.

### H-02: Broker-Accept Command Delivery, Not PLC-Confirmed Delivery

1. Is the finding actually reproducible?

The at-most-once behavior is reproducible by source design. The claimed operational failure, PLC missing the command, is contradicted by verified BBA facts: BEB publishes correctly, subscriber receives the payload, PLC receives the payload, D340 updates, and print job starts.

2. Is it proven from source code?

Yes. `app/mqtt_client.py:148-153` publishes with QoS 0 and retain false. `app/api.py:179-195` marks API command success after MQTT publish result success.

3. Is it proven from runtime?

No failure is proven from runtime. Runtime proves the current BBA path works end to end.

4. Is it based only on static analysis?

The risk is static-analysis based. It is a valid protocol limitation, not an observed production failure.

5. Could it be a false positive?

It is a false positive as a blocker because runtime evidence proves PLC receipt and print start. It is not a false positive as an architectural limitation.

6. Could it already be mitigated elsewhere?

Partially. PLC MQTT connection, PINGREQ/PINGRESP, BEB reconnect, Docker restart, and Mosquitto persistence are verified. Those reduce common failure cases, but do not create PLC-level acknowledgement.

7. Does another module already prevent this problem?

No module confirms the PLC applied an Odoo-to-PLC command. The FastAPI `api_commands` table records broker publish result, not PLC application result.

Challenge result:

This is a production risk only if the business requirement is guaranteed PLC command application. For the verified current workflow, it is acceptable.

### H-03: ACK Publish Failure Is Not Durable or Retried

1. Is the finding actually reproducible?

It is reproducible by code inspection. A runtime failure was not provided. The verified runtime facts show PLC communication and print job behavior working.

2. Is it proven from source code?

Yes. `app/queue_worker.py:418-429` marks the row completed and then calls `_publish_ack(ack)`. `app/queue_worker.py:431-436` does not inspect publisher failure. `app/mqtt_client.py:382-398` returns `False` on publish failure.

3. Is it proven from runtime?

No. No verified runtime fact shows ACK publish failure.

4. Is it based only on static analysis?

Yes, for the failure condition. The code path is real, but the operational failure is hypothetical unless the broker/network fails at that exact point.

5. Could it be a false positive?

No as a code-path risk. Yes if treated as a current production blocker, because the runtime evidence shows the deployed path is functioning.

6. Could it already be mitigated elsewhere?

Partially. Duplicate ACK replay exists. If PLC republishes the same completed message, `app/mqtt_client.py:266-344` can replay the stored ACK, with replay throttling in `app/database.py:732-805`.

7. Does another module already prevent this problem?

No module durably retries ACK publication after a publish failure. Duplicate ACK replay is a partial recovery mechanism, not a direct ACK outbox.

Challenge result:

This is a real reliability risk, but not a confirmed production blocker. It should be tracked as a targeted improvement after controlled deployment unless PLC ACK loss is shown to block production.

### H-04: Native and Docker Deployment Conflict

1. Is the finding actually reproducible?

Not from the provided runtime facts. The repository contains native deployment documentation, but no evidence was provided that a native BEB or native Mosquitto service is actually running on BBA.

2. Is it proven from source code?

Only the possibility is proven. README contains a native systemd example at `README.md:669-683`; Docker Compose defines one `beb` container and one `beb-mosquitto` container at `docker-compose.yml:7` and `docker-compose.yml:40`.

3. Is it proven from runtime?

No. The verified runtime facts show Docker restart, Ubuntu reboot, SQLite persistence, Mosquitto persistence, BEB reconnect, PLC receive, D340 update, and print job start. Those facts do not show duplicate BEB instances.

4. Is it based only on static analysis?

Yes. It is a deployment hygiene concern inferred from documentation coexistence.

5. Could it be a false positive?

Yes as a production blocker. The presence of native documentation does not prove a native process exists.

6. Could it already be mitigated elsewhere?

Yes. If BBA/BBW only enable the Docker stack, the risk is mitigated operationally. The verified Docker restart and reboot behavior suggest Docker is the active reference path.

7. Does another module already prevent this problem?

No code module prevents an operator from starting a second BEB instance outside Compose. Compose itself defines only one BEB container.

Challenge result:

This is not a code defect and not a proven production blocker. It is a deployment checklist item.

## Reclassified Findings

| Finding ID | Original severity | New category | New production meaning | Why |
|---|---|---|---|---|
| H-01 | High | B. Production Risk, acceptable with operational controls | Not a blocker for controlled BBW deployment | Real security exposure, but LAN exposure is required and verified for PLC operation. Control it at LAN/firewall/OT-network level. |
| H-02 | High | B. Production Risk, acceptable with operational controls | Not a blocker for current workflow | Runtime proves PLC receives command and print starts. The remaining risk is lack of PLC-confirmed delivery in offline edge cases. |
| H-03 | High | B. Production Risk, acceptable with operational controls | Not a blocker unless ACK loss is observed to stop production | Source-proven reliability gap, but not runtime-proven. Duplicate ACK replay partially mitigates repeated PLC messages. |
| H-04 | High | C. Maintainability / deployment hygiene issue | Not a blocker | Static-only concern. No evidence of multiple runtime instances. Keep as deployment checklist item. |

No finding reclassifies to A, Confirmed Production Blocker.

No finding reclassifies to F, False Positive, as a finding. H-02 and H-04 were false positives only in their earlier treatment as blockers.

## Verified Runtime Evidence

The following operator-provided BBA runtime facts are treated as verified:

| Runtime fact | Effect on audit findings |
|---|---|
| Docker Mosquitto exposed on LAN | Confirms H-01 exposure is intentional and working |
| Port 1883 reachable | Confirms Docker port binding is correct |
| PLC connects successfully as `FX5U_TEST` | Refutes functional PLC connectivity blocker |
| MQTT PINGREQ/PINGRESP working | Confirms stable MQTT client session behavior |
| BEB publishes correctly | Refutes command formatting/publish-path blocker |
| MQTT subscriber receives correct payload | Confirms Mosquitto receives and distributes BEB command |
| PLC finally receives payload | Refutes H-02 as an observed blocker |
| D340 updated | Confirms PLC applies received model payload |
| Print job started | Confirms current Odoo-to-PLC workflow functions |
| Docker restart working | Reduces deployment reliability concern |
| Ubuntu reboot working | Confirms restart policy/host reboot path |
| SQLite persistence verified | Confirms BEB data volume behavior |
| Mosquitto persistence verified | Confirms broker persistence volume behavior |
| BEB reconnect verified | Confirms MQTT reconnect behavior operationally |

Contradictions with previous High findings:

- H-02 said a command can be missed by the PLC. That remains a theoretical QoS 0/offline risk, but verified runtime now proves the current PLC receives and applies commands.
- H-04 implied duplicate/native process risk could block deployment. Verified Docker restart, reboot, persistence, reconnect, and PLC behavior do not support an active duplicate-instance problem.
- H-01 remains true as exposure, but the same exposure is now proven necessary for PLC connectivity.
- H-03 is not contradicted; it remains a narrow failure-mode risk, not a proven current failure.

## Architecture Assessment

Current role:

```text
PLC
 |
 v
MQTT / Mosquitto
 |
 v
BEB
 |
 v
SQLite
 |
 v
Odoo XML-RPC
 |
 v
BEB
 |
 v
MQTT / Mosquitto
 |
 v
PLC
```

The architecture is appropriate for the current industrial edge middleware role. It is not unnecessarily cloud-native, distributed, or platform-heavy. It uses:

- Mosquitto for PLC-compatible MQTT transport.
- SQLite for local durable transaction state.
- A worker and watchdog for Odoo outage/retry behavior.
- A temporary child process for killable Odoo XML-RPC calls.
- FastAPI for controlled Odoo-to-PLC push commands.
- Docker Compose for repeatable BBA/BBW deployment.

The design is more complex than a simple MQTT script, but that complexity directly supports industrial reliability: persistence, duplicate suppression, timeout isolation, restart recovery, and readiness signaling.

## Process and Instance Review

Does BEB actually run multiple permanent instances?

No evidence says yes. Source and Compose define a single BEB application container and one application entry point.

Evidence:

- `docker-compose.yml:2-36` defines one `beb` service.
- `app/main.py:97-124` creates one MQTT client, one optional Odoo worker, one readiness monitor, and one API server.
- `app/queue_worker.py:178-183` prevents starting the same worker object twice.
- `app/readiness.py:144-152` prevents starting the same readiness monitor twice.
- `app/api_server.py:45-48` prevents starting the same API server object twice.

Are repeated `auto-xxxxxxxx` MQTT clients health checks?

Yes, based on source and Compose, they are most likely Docker health-check clients.

Evidence:

- `docker-compose.yml:50-67` runs `mosquitto_pub` every 30 seconds.
- Each `mosquitto_pub` invocation is a temporary MQTT client.
- The original audit report also identified the Mosquitto health-check client as temporary every 30 seconds.

There is no evidence that these clients are additional BEB application instances.

Does BEB create unnecessary background workers?

No. Permanent threads/tasks are directly tied to runtime responsibilities:

| Permanent thread/task | Source evidence | Purpose | Necessary? |
|---|---|---|---|
| Main process wait | `app/main.py:121-125` | Keeps process alive after starting services | Essential |
| MQTT paho loop | `app/mqtt_client.py:84-97` | Broker IO, reconnect, message callbacks | Essential |
| Odoo queue worker | `app/queue_worker.py:190-203` | Submits durable SQLite rows to Odoo | Essential when Odoo enabled |
| Stale watchdog | `app/queue_worker.py:196-203`, `app/queue_worker.py:267-274` | Recovers stale PROCESSING rows | Useful and reliability-critical |
| Readiness monitor | `app/readiness.py:144-167`, `app/readiness.py:201-207` | Publishes debounced BR state | Useful; essential if PLC uses BR |
| Uvicorn API thread | `app/api_server.py:57-68` | Handles Odoo-to-PLC API | Essential when API enabled |

Temporary processes:

- Odoo child submission process: `app/queue_worker.py:476-563`, `app/queue_worker.py:895-922`.
- BEB Docker health check: `docker-compose.yml:27-36`.
- Mosquitto Docker health check: `docker-compose.yml:50-67`.

These are not permanent application instances.

## Resource Assessment

| Module/artifact | Rating | Explanation |
|---|---|---|
| `app/main.py` | Essential | Single startup/shutdown coordinator |
| `app/config.py` | Essential | Centralizes deployment environment |
| `app/mqtt_client.py` | Essential | Core PLC/BEB MQTT bridge |
| `app/message_parser.py` | Essential | Validates PLC-to-Odoo message contract |
| `app/command_parser.py` | Essential | Validates Odoo-to-PLC command contract |
| `app/database.py` | Essential | Durable queue, duplicate protection, API audit |
| `app/queue_worker.py` | Essential | Odoo submission, retry handling, timeout isolation, ACK handling |
| `app/odoo_client.py` | Essential | XML-RPC integration with timeout transport |
| `app/api.py` | Essential | Odoo-to-PLC command endpoint and health |
| `app/api_server.py` | Useful | Keeps Uvicorn embedded in one process without extra workers |
| `app/readiness.py` | Useful | Important if PLC logic consumes BR; otherwise optional |
| Dockerfile | Essential | Reference deployment image |
| `docker-compose.yml` | Essential | Reference deployment topology |
| Mosquitto config | Essential | Broker listener and persistence |
| Tests | Useful | Strong regression coverage |
| Scripts | Optional | Manual validation helpers |
| Native systemd docs | Optional | Rollback/reference only; should not be active with Docker |
| Odoo sample/reference folders | Future | Reference material, not runtime |

Is BEB over-engineered?

No. For the current industrial role, the mechanisms are proportionate:

- SQLite is justified because PLC-to-Odoo messages must survive restart and prevent duplicates.
- Child-process isolation is justified because Odoo XML-RPC can hang or exceed business-safe timeouts.
- Stale PROCESSING recovery is justified because crashes can happen mid-transaction.
- Duplicate ACK replay is justified because PLC MQTT delivery can duplicate.
- Readiness is justified if PLC logic depends on BEB/Odoo availability.

The only component that may be tuned down is readiness frequency if Odoo login volume becomes noisy. That is tuning, not over-engineering.

## Production Blockers

Confirmed production blockers: none.

The original High findings do not meet the threshold for "Confirmed Production Blocker" after applying the provided runtime facts.

| Finding | Can cause duplicate production update? | Lost production update? | Wrong ACK? | Incorrect model printing? | SQLite corruption? | Docker deployment failure? | PLC communication failure? | Odoo inconsistency? | Blocker? |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| H-01 | Possible only through unauthorized LAN actor | Possible only through malicious/disruptive LAN actor | Possible only through spoofing | Possible only through spoofing | No source evidence | No | No, runtime proves PLC comms | Possible only through spoofing | No, controlled-network risk |
| H-02 | No | Possible if PLC offline at publish time | No | Possible only if command missed/stale operationally | No | No | Possible in offline/unsubscribed edge case; runtime currently passes | No | No, accepted delivery model risk |
| H-03 | No | No Odoo update loss; possible ACK loss | No wrong ACK; possible missing ACK | No | No | No | Possible missing ACK only | Possible Odoo-complete/PLC-not-ACKed state | No, reliability improvement |
| H-04 | Possible only if a second runtime is actually started | Possible only if conflict exists | Possible only under duplicate runtime | Possible only under duplicate runtime | No direct evidence | Possible port conflict if native Mosquitto active | Possible if conflict exists | Possible if duplicate BEB uses separate DB | No, deployment checklist |

H-03 is the closest real reliability concern, but the failure requires a specific post-Odoo-success MQTT publish failure. It is not proven by runtime, and duplicate ACK replay partially mitigates duplicate PLC sends.

## False Positives

No original High finding is a complete false positive as a technical observation.

The following were false positives as production blockers:

| Finding | False-positive aspect |
|---|---|
| H-02 | The earlier report treated "PLC may miss command" as a blocker. Verified runtime proves PLC receives payload, D340 updates, and print starts. |
| H-04 | The earlier report treated coexistence of native docs and Docker as a blocker. Verified runtime does not show multiple BEB instances or service conflict. |

The following were valid findings but overstated as mandatory blockers:

| Finding | Overstatement |
|---|---|
| H-01 | LAN MQTT exposure is a real security risk, but it is also required for this PLC architecture and is acceptable on a controlled industrial LAN. |
| H-03 | ACK publish durability is a real reliability gap, but not a verified production failure or deployment blocker. |

## Recommended Corrections

### P0: Mandatory Before BBW

None from the challenged Critical/High findings.

There are no source-proven plus runtime-proven blockers that require code/config changes before BBW controlled deployment.

### P1: Recommended Before BBW

These are recommended operational controls, not blockers:

| Item | Reason |
|---|---|
| Confirm BBW will run only the Docker BEB stack | Prevents the H-04 hypothetical native/Docker conflict |
| Keep FastAPI host binding restricted to `127.0.0.1:8000:8000` | Preserves API exposure boundary |
| Keep Mosquitto reachable only on the trusted PLC/BEB LAN | H-01 is acceptable only with network trust or controls |
| Confirm BBW `.env` quotes secrets containing `$` | Avoids Compose interpolation mistakes |
| Record the accepted MQTT delivery contract | H-02 is acceptable if at-most-once command delivery is understood |
| Confirm PLC behavior if ACK is missed | Determines whether H-03 needs immediate ACK outbox work |

### P2: After BBW Deployment

| Item | Reason |
|---|---|
| Add ACK publish state or bounded ACK retry if production shows missed ACKs | Addresses H-03 reliability gap if it matters operationally |
| Reduce Mosquitto health-check log noise if logs are noisy | Repeated `auto-xxxxxxxx` clients are health checks, not extra BEB instances |
| Align `.env.example` and README with hard `1883:1883` exposure | Prevents operator confusion |
| Add Docker log rotation/resource limits | Operational hardening |
| Add retention/archive policy for SQLite audit history | Long-running maintenance |

### P3: Future Platform Evolution

| Item | Reason |
|---|---|
| MQTT ACLs/auth/TLS if PLC and site constraints allow | Security hardening beyond controlled LAN |
| PLC-level command confirmation workflow if business requires it | Upgrades H-02 from at-most-once to confirmed command execution |
| Real-broker integration tests | Stronger regression confidence |
| Separate liveness and detailed health endpoint | API hardening if exposure expands |

## Nice-to-have Improvements

These are not required for controlled BBW deployment:

- Kubernetes, cloud-native architecture, PostgreSQL, and microservices are not recommended for the current BEB role.
- Replacing SQLite is not justified for current throughput and edge durability needs.
- Splitting services beyond BEB plus Mosquitto is not justified.
- Adding a dashboard can wait until platform expansion.
- Retained readiness or periodic BR publication can wait unless PLC restart tests show stale readiness behavior.

## Final Production Decision

Decision: READY FOR CONTROLLED PRODUCTION DEPLOYMENT.

Why this decision is supported:

- Source code shows a single BEB application process model with guarded worker/readiness/API starts.
- Docker Compose defines one BEB container and one Mosquitto container.
- Verified runtime evidence confirms Mosquitto LAN exposure, PLC connectivity, MQTT keepalive, BEB publish, PLC receipt, D340 update, print start, Docker restart, Ubuntu reboot, SQLite persistence, Mosquitto persistence, and BEB reconnect.
- No Critical findings exist.
- No original High finding is both source-proven and runtime-proven as a current blocker.
- Remaining risks are operationally controllable within a trusted industrial LAN and controlled Docker deployment.

Based on source code, runtime evidence, Docker validation, and production workflow verification, BEB is READY FOR CONTROLLED PRODUCTION DEPLOYMENT for BBW deployment.
