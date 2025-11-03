# Streams Event Router POC (Tyk Streams) — Design, Docs, and Demo

This repository delivers an “Event Gateway” built with Tyk Streams that:
- Accepts events via RabbitMQ (AMQP 0.9) and HTTP
- Validates and normalizes CloudEvents when appropriate
- Applies routing decisions in processors (business‑level types)
- Applies sink‑specific transformations in outputs (Kafka → Protobuf; others → JSON)
- Fan‑outs to multiple sinks with a single event

It comes with a self‑contained docker‑compose stack and a demo UI. All routing happens inside the Gateway via the x‑tyk‑streaming OAS extension.

—

## Architecture

High‑level components (current implementation)

```mermaid
flowchart TD
  subgraph Clients
    UI[Demo UI]
    Curl[curl / Postman]
  end

  subgraph Tyk Gateway
    API1["/Streams API<br/>(/streams-api)/"]
  end


  subgraph "Streams (inside Gateway)"
    S1[["amqp_ingest<br/>AMQP background"]]
    S2[["http_ingest<br/>HTTP path /event"]]
  end

  subgraph Brokers
    RQ[(RabbitMQ)]
    K[(Kafka)]
  end

  UI -->|POST /streams-api/event| API1
  Curl -->|POST /streams-api/event| API1
  RQ -->|consume inbound-queue| S1

  S1 -->|ORDER_CREATED<br/>→ Protobuf| K
  S1 -->|USER_REGISTERED<br/>→ JSON| RQ
  S1 -->|AUDIT_LOG<br/>→ JSON| UI

  S2 -->|same routing & transforms| K
  S2 -->|same routing & transforms| RQ
  S2 -->|same routing & transforms| UI
```

Design decisions
- Two streams, one API: a background AMQP stream (`amqp_ingest`) and an HTTP stream (`http_ingest`). Both share the same routing/transforms.

—

## Routing and transforms (at a glance)

```mermaid
flowchart LR
  A[Incoming Event] --> B{CE shaped?}
  B -- yes --> V["Validate (JSON Schema)<br/>Normalize fields"]
  B -- no --> P[Pass through]
  V --> R
  P --> R
  R{Type}
  R -- ORDER_CREATED --> Kf["meta(route_kafka)=true"]
  R -- USER_REGISTERED --> Aq["meta(route_amqp)=amqp_local"]
  R -- AUDIT_LOG --> Ht["meta(route_http)=true"]
  R -- BROADCAST_DEMO --> All[set all three]
  All --> O[Output switch<br/>continue:true]
  Kf --> O
  Aq --> O
  Ht --> O
  O -->|Kafka| KP[Encode Protobuf event.Event]
  O -->|AMQP| AJ[Send JSON]
  O -->|HTTP| HH[Send JSON + route_hint]
```

—

## Key configs (embedded excerpts)

1) Streams OAS — two streams under one API (YAML, shortened)

```yaml
openapi: 3.0.3
servers:
  - url: "http://tyk-gateway:8282/streams-api/"
x-tyk-api-gateway:
  info:
    name: Event Router (Streams)
    orgId: default
    state:
      active: true
      internal: false
  server:
    listenPath:
      strip: true
      value: /streams-api/
x-tyk-streaming:
  streams:
    amqp_ingest:
      input:
        amqp_0_9:
          urls: [ "amqp://guest:guest@rabbitmq:5672/" ]
          queue: inbound-queue
          prefetch_count: 64
      pipeline:     # CE validation + routing switches
        processors: [ ... ]
      output:
        switch:      # Kafka→Protobuf, AMQP/HTTP→JSON
          cases: [ ... ]
    http_ingest:
      input:
        http_server:
          path: /event
          allowed_verbs: [ POST ]
          timeout: 20s
      pipeline:     # same CE + routing
        processors: [ ... ]
      output:
        switch:      # same outputs
          cases: [ ... ]
```

2) Routing/transform excerpt (from the equivalent local config)

```yaml
pipeline:
  threads: -1
  processors:
    - switch:
        - check: 'this.specversion.string().catch("") != ""'
          processors:
            - json_schema:
                schema: |
                  {"$schema":"http://json-schema.org/draft-07/schema#","type":"object",
                   "required":["id","source","type"],
                   "properties":{"id":{"type":"string"},"source":{"type":"string"},
                                 "type":{"type":"string"},"data":{"type":["object","array","string","number","boolean","null"]}}}
            - mapping: |
                root.id = this.id.string()
                root.source = this.source.string()
                root.type = this.type.string().uppercase()
                root.data = this.data
                root.timestamp = now().ts_format("2006-01-02T15:04:05Z07:00")
                meta is_ce = "true"
        - processors:
            - mapping: 'meta is_ce = "false"'

    - switch:
        - check: 'this.type.string().catch("").uppercase() == "BROADCAST_DEMO"'
          processors:
            - mapping: |
                meta route_kafka = "true"
                meta route_amqp = "amqp_local"
                meta route_http = "true"
        - check: 'this.type.string().catch("").uppercase() == "ORDER_CREATED"'
          processors:
            - mapping: 'meta route_kafka = "true"'
        - check: 'this.type.string().catch("").uppercase() == "USER_REGISTERED"'
          processors:
            - mapping: 'meta route_amqp = "amqp_local"'
        - check: 'this.type.string().catch("").uppercase() == "AUDIT_LOG"'
          processors:
            - mapping: 'meta route_http = "true"'
        - processors:
            - mapping: 'meta route_http = meta("route_http").or("true")'

output:
  switch:
    cases:
      - check: 'meta("route_kafka") == "true"'
        continue: true
        output:
          kafka: { addresses: [ "labs-kafka:9092" ], topic: "high-priority-topic", max_in_flight: 64 }
          processors:
            - mapping: |
                root.id = this.id.string().catch(uuid_v4())
                root.source = this.source.string().catch("unknown")
                root.type = this.type.string().catch("UNKNOWN").uppercase()
                root.data = this.data.or(this)
                root.timestamp = now().ts_format("2006-01-02T15:04:05Z07:00")
            - protobuf: { operator: from_json, message: event.Event, import_paths: [ "./schemas" ] }
      - check: 'meta("route_amqp") == "amqp_local"'
        continue: true
        output:
          amqp_0_9: { urls: [ "amqp://guest:guest@rabbitmq:5672/" ], exchange: "", key: "low-priority-queue", max_in_flight: 64 }
      - check: 'meta("route_http").or("false") == "true"'
        output:
          http_client: { url: "http://demo:8080/events", verb: POST, headers: { Content-Type: application/json }, max_in_flight: 64 }
          processors:
            - mapping: |
                root = this
                root.type = this.type.or("unknown")
                root.route_hint = {"kafka": meta("route_kafka").or("false"), "amqp": meta("route_amqp").or(""), "http": meta("route_http").or("false") }
```

—

## How the demo works

- UI (http://localhost:8080) offers buttons that POST JSON CloudEvents to the Streams API.
- The server POSTs to `TYK_STREAMS_URL` (inside Docker: `http://tyk-gateway:8282/streams-api/event`). No fallback is used.
- Outputs render in real time:
  - Kafka: Protobuf decoded to JSON via generated Go types from `schemas/`.
  - RabbitMQ: JSON lines.
  - HTTP: the demo collects at `/events` and displays bodies.

—

## Run it

Prereqs: Docker. Then:
- Start: `docker compose up -d --build`
- UI: http://localhost:8080
- Streams API (host): http://localhost:18282/streams-api
- Admin API (host):   http://localhost:19696/tyk/apis

Smoke test (no trailing slash)
```bash
curl -i -H 'Content-Type: application/json' \
  --data '{"id":"1","source":"demo","type":"ORDER_CREATED","specversion":"1.0","data":{}}' \
  http://localhost:18282/streams-api/event
```

Stop:
- `docker compose down -v --remove-orphans`

—

## Troubleshooting tips

- Trailing slash
  - The HTTP stream binds `/event` (no trailing slash). `/streams-api/event/` will 404.

- First probe 404
  - The importer creates APIs on startup. If a request hits before import, 404 is expected once; re‑emit or `docker compose run --rm tyk-import` to force re‑import.

- Duplicate listen paths
  - The importer deletes any pre‑existing Streams APIs (by name and by listen path prefix) to avoid auto‑suffixed paths.

- RabbitMQ readiness
  - Queues are declared by `rabbit-init` before import; transient `NOT_FOUND - inbound-queue` errors should disappear on next retry.

- Protobuf mapping
  - Gateway mounts `./schemas` to `/work/schemas` so the `protobuf` processor can load descriptors.

—

## Repository layout

- `tyk/oas/streams-router.oas.yaml` — Tyk Streams OAS (two streams)
- `tyk/import_oas.sh` — curl‑only importer: YAML→JSON, dedup + import + reload
- `docker-compose.yml` — Gateway (ports 18282/19696), RabbitMQ, Kafka/ZooKeeper, demo UI
- `schemas/` — Protobuf schemas (event + testing.Person)
- `definitions.json` — RabbitMQ definitions (declares required queues)
- `demo/` — UI and server (SSE + Protobuf decode)
 
- `schemas/` — `.proto` files
- `docker-compose.yml`, `Makefile`

—

If you want this README content reflected inside the UI (e.g., an “About” panel with the same diagrams and code excerpts), say the word and I’ll wire it in.
