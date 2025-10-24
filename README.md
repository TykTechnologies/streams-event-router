# Event Router POC with Bento — Concepts, Architecture, and Rationale

This repository implements an “Event Gateway” with Bento that ingests events, validates them when appropriate, routes them based on content, applies per‑destination transformations, and delivers to multiple targets. It also includes a demo UI for manual, reproducible flows and decoded views of messages.

The configuration emphasizes a clean separation of responsibilities:
- Routing logic lives in the processors phase (fast, testable, sink‑agnostic).
- Transformation logic lives inside each output case (tailored per sink).

---

## Why This Design

1) Processors decide routing; outputs transform
- Processors set explicit metadata flags (e.g., `meta("route_kafka") = "true"`) according to event content.
- Output cases consult those flags, use `continue: true` to support fan‑out, and apply only the transforms required by that destination (e.g., Kafka → Protobuf; others → JSON).

2) Conditional CloudEvents handling
- Validation and normalization occur only when payloads look like CloudEvents (CE). Non‑CE JSON passes through unchanged.
- This supports heterogeneous inputs without forcing a single event envelope.

3) Manual, reproducible demo
- No background traffic. All events are created by clicking in the UI, which makes behavior easy to observe and explain.

---

## Architecture Overview

- Inputs
  - RabbitMQ (`amqp_0_9`): consumes from `inbound-queue`.
  - HTTP (`http_server`): accepts POST at `/inbound`, holds the request up to 20s while the pipeline completes.

- Pipeline (processors)
  - CE detection + validation (only when CE‑shaped):
    - Normalizes to an Event JSON: `{id, source, type, data, timestamp}`.
    - Sets `meta("is_ce") = "true"`.
  - Routing switch (business terms):
    - `ORDER_CREATED` → `meta("route_kafka") = "true"`.
    - `USER_REGISTERED` → `meta("route_amqp") = "amqp_local"`.
    - `AUDIT_LOG` → `meta("route_http") = "true"`.
    - `BROADCAST_DEMO` → sets all three for fan‑out.
    - Default → ensures `meta("route_http")` is true so messages are not dropped.

- Outputs (delivery + per‑sink transforms)
  - Output switch with `continue: true` to support multi‑destination:
    - Kafka case: wraps to Event JSON (if needed) and encodes with Protobuf (`schemas/event.proto`).
    - AMQP case: forwards JSON unchanged.
    - HTTP case: forwards JSON unchanged and adds a `route_hint` field for clarity.

Why this arrangement works well
- Routing is centralized, simple, and sink‑independent.
- Each sink is responsible for its own format (e.g., Protobuf for Kafka only), which avoids hidden cross‑effects.
- Fan‑out is a natural extension: add `continue: true` and a new case without duplicating routing checks.

---

## Event Types and Demo Scenarios

- `ORDER_CREATED` → Kafka, encoded as Protobuf `event.Event`.
- `USER_REGISTERED` → RabbitMQ (JSON).
- `AUDIT_LOG` → HTTP collector (JSON).
- `BROADCAST_DEMO` → Kafka + RabbitMQ + HTTP (fan‑out).
- Person JSON (non‑CE) → default HTTP route, to demonstrate heterogeneous input handling.

---

## UI and Developer Experience

Open `http://localhost:8080` after starting the stack.

- Legend table (top)
  - Left column explains the scenario; right column emits a single event.
  - Includes an “Inbound AMQP” row to push directly to `inbound-queue`.

- Panels
  - Inputs: HTTP Input status; AMQP inbound status + any inbound messages.
  - Outputs: Kafka (decoded Protobuf), RabbitMQ (JSON), HTTP collector (JSON + `route_hint`).

- Kafka Protobuf decoding
  - The demo image generates Go code from `schemas/` at build time and decodes `event.Event` to JSON for display.
  - Modify `schemas/*.proto` → rebuild the demo image to reflect changes.

---

## Running Locally (Self‑Contained)

This compose stack runs: Zookeeper, Kafka (+topic init), RabbitMQ (AMQP 1.0 enabled), Bento (`streams`), Demo (`demo`). Bento waits for brokers before starting.

- Start
  - `make compose-up`
  - UI: `http://localhost:8080`
  - Bento HTTP input: `http://localhost:4195/inbound`

- Stop
  - `make compose-down`

Notes
- The RabbitMQ definitions already create the queues.
- Kafka topics (`high-priority-topic`, `FOO`) are auto‑created by the `kafka-init` container.

---

## Configuration Walkthrough (bento.local.yaml)

- Inputs
  - `amqp_0_9`: consumes `inbound-queue` without redeclaring (avoids durability mismatch errors).
  - `http_server`: `timeout: 20s` to accommodate any work in the pipeline.

- Processors
  - `switch` (CE detection): validates CE only if present and normalizes to a stable Event JSON shape.
  - `switch` (routing): sets `meta("route_*")` flags by business event type. This is the single source of truth for routing decisions.

- Outputs
  - `switch` with `continue: true`: delivers to every matching case:
    - Kafka: Protobuf encode via processors in this case only.
    - AMQP: JSON.
    - HTTP: JSON, adds `route_hint` for clarity.

---

## Flipping to Cloud Targets (Optional)

Use `bento.yaml` and set env vars:
- Event Hubs (Kafka API): `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TLS_ENABLED=true`, `KAFKA_SASL_*`, `KAFKA_TOPIC_HIGH`.
- Service Bus (AMQP 1.0): `AMQP_MODE=azure`, `AZURE_SB_*`.
- Event Grid (HTTP): `DEFAULT_HTTP_URL`, `AZURE_EG_KEY`.

---

## Troubleshooting

- HTTP emit timeouts
  - Bento’s `http_server.timeout` is 20s. If you add heavy processors, increase it.
  - The demo POST timeout is 12s.

- RabbitMQ durable mismatch
  - We do not redeclare `inbound-queue`. Queues are pre‑created via `definitions.json`.

- DNS / ports
  - The demo tries multiple Bento URLs (`BENTO_URL` → `streams` → `host.docker.internal` → `localhost`).

- Linting
  - `make lint` or run Bento docker: `ghcr.io/warpstreamlabs/bento:latest -c bento.local.yaml lint`.

---

## Project Layout

- `bento.local.yaml` — local routing + transforms (Kafka Protobuf; JSON for AMQP/HTTP).
- `bento.yaml` — parity config with optional cloud outputs.
- `schemas/` — Protobuf definitions (vendored minimal WKTs under `schemas/google/protobuf`).
- `streams/` — Bento wrapper (waits for Kafka/RabbitMQ before starting).
- `demo/` — UI + server; decodes Kafka Protobuf to JSON.
- `docker-compose.yml` — self‑contained stack.
- `scripts/discover_ports.sh` — env discovery for standalone Bento runs.

---

## Appendix: Commands

- Lint Bento config: `make lint`
- Run Bento locally: `make run` (or `make run-docker`)
- Start demo stack: `make compose-up`
- Stop and clean: `make compose-down`

If you’d like the README concepts to appear within the UI (e.g., a collapsible “About” panel or a live “Routing Rules” card built from a small JSON next to the config), we can add that next.

