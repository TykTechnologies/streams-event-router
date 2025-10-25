# Streams Event Router POC (Bento + Tyk) — Design, Docs, and Demo

This repository delivers an “Event Gateway” that:
- Accepts events via RabbitMQ (AMQP 0.9) and HTTP
- Validates and normalizes CloudEvents when appropriate
- Applies routing decisions in processors (business‑level types)
- Applies sink‑specific transformations in outputs (Kafka → Protobuf; others → JSON)
- Fan‑outs to multiple sinks with a single event

It comes with a self‑contained docker‑compose stack and a demo UI.

—

## Architecture

High‑level components

```mermaid
flowchart TD
  subgraph Clients
    UI[Demo UI]
    Curl[curl / Postman]
  end

  subgraph Tyk Gateway
    API1[/Streams API\n(x-tyk-streaming)/]
    API2[/HTTP Ingest Proxy\n(/ingest-proxy/ → Bento)/]
  end

  subgraph Bento (streams)
    S1[[bento_router\nAMQP background]]
    S2[[http_ingest\nHTTP path]]
  end

  subgraph Brokers
    RQ[(RabbitMQ)]
    K[(Kafka)]
  end

  UI -->|POST events| API2
  Curl -->|POST events| API2
  API2 -->|proxy| S2
  RQ -->|consume inbound-queue| S1

  S1 -->|ORDER_CREATED\n→ Protobuf| K
  S1 -->|USER_REGISTERED\n→ JSON| RQ
  S1 -->|AUDIT_LOG\n→ JSON| UI

  S2 -->|same routing & transforms| K
  S2 -->|same routing & transforms| RQ
  S2 -->|same routing & transforms| UI
```

Design decisions
- Two streams, one API: We split Streams into two logical pipelines but keep a single Tyk API.
  - `bento_router` is a background job (AMQP only) — it should start immediately and keep running.
  - `http_ingest` handles HTTP events — it mirrors the same routing and transformations as `bento_router`.
- Stable HTTP entrypoint: In practice, http_server embedding under Streams can vary by build. To guarantee a reliable endpoint, we expose `/ingest-proxy/` as a standard Tyk proxy that forwards to Bento’s `/inbound` HTTP input.

—

## Routing and transforms (at a glance)

```mermaid
flowchart LR
  A[Incoming Event] --> B{CE shaped?}
  B -- yes --> V[Validate (JSON Schema)\nNormalize fields]
  B -- no --> P[Pass through]
  V --> R
  P --> R
  R{Type}
  R -- ORDER_CREATED --> Kf[meta(route_kafka)=true]
  R -- USER_REGISTERED --> Aq[meta(route_amqp)="amqp_local"]
  R -- AUDIT_LOG --> Ht[meta(route_http)=true]
  R -- BROADCAST_DEMO --> All[set all three]
  All --> O[Output switch\ncontinue:true]
  Kf --> O
  Aq --> O
  Ht --> O
  O -->|Kafka| KP[Encode Protobuf event.Event]
  O -->|AMQP| AJ[Send JSON]
  O -->|HTTP| HH[Send JSON + route_hint]
```

—

## Key configs (embedded excerpts)

1) Streams OAS — two streams under one API (shortened for readability)

```json
{
  "openapi": "3.0.3",
  "servers": [{ "url": "http://tyk-gateway:8282/streams-api/" }],
  "x-tyk-api-gateway": {
    "info": { "name": "Bento Router (Streams)", "state": { "active": true } },
    "server": { "listenPath": { "strip": true, "value": "/streams-api/" } }
  },
  "x-tyk-streaming": {
    "allow_unsafe": ["http_server"],
    "streams": {
      "bento_router": {
        "input": { "broker": { "inputs": [ { "amqp_0_9": { "urls": ["amqp://guest:guest@rabbitmq:5672/"], "queue": "inbound-queue", "prefetch_count": 64 } } ] } },
        "pipeline": { "processors": [ /* CE validation + routing switches */ ] },
        "output": { "switch": { /* Kafka→Protobuf, AMQP/HTTP→JSON */ } }
      },
      "http_ingest": {
        "input": { "broker": { "inputs": [ { "http_server": { "path": "/inbound", "allowed_verbs": ["POST"], "timeout": "20s" } } ] } },
        "pipeline": { "processors": [ /* same CE + routing */ ] },
        "output": { "switch": { /* same outputs */ } }
      }
    }
  }
}
```

2) Bento pipeline (excerpt from `configs/bento/bento.local.yaml`)

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

- UI (http://localhost:8080) offers buttons that POST JSON events.
- The server posts to the Gateway first at `/ingest-proxy/`.
  - Gateway proxies to Bento’s HTTP input at `/inbound`.
  - If the Gateway path were unavailable, the app falls back to Bento directly to keep the demo smooth.
- Outputs render in real time:
  - Kafka: Protobuf decoded into JSON (generated Go types from `schemas/`).
  - RabbitMQ: JSON lines.
  - HTTP: the demo collects and displays the request body.

—

## Run it

Prereqs: Docker. Then:
- Start: `make compose-up`
- UI: http://localhost:8080
- Gateway HTTP ingest: `POST http://localhost:8282/ingest-proxy/`

Smoke test
```bash
curl -i -H 'Content-Type: application/json' \
  --data '{"id":"1","source":"demo","type":"AUDIT_LOG","specversion":"1.0","data":{}}' \
  http://localhost:8282/ingest-proxy/
```

Stop:
- `make compose-down`

—

## Troubleshooting tips

- RabbitMQ not ready yet
  - You might briefly see connection refused; the AMQP stream retries until `inbound-queue` is available.

- HTTP path returns 404
  - Use the supported endpoint `/ingest-proxy/` (Gateway proxy → Bento HTTP input).

- Protobuf path
  - Gateway mounts `./schemas` at `/work/schemas` so Protobuf encoding works from inside the Streams pipeline.

—

## Repository layout

- `configs/bento/bento.local.yaml` — local pipeline and routing (Kafka Protobuf; AMQP/HTTP JSON)
- `configs/bento/bento.yaml` — optional cloud flip config
- `tyk/oas/bento-router.oas.json` — Streams OAS (two streams)
- `tyk/apps/ingest-proxy.json` — Gateway classic API: `/ingest-proxy/` → Bento HTTP input
- `demo/` — UI and server (SSE + Protobuf decode)
- `streams/` — Bento wrapper
- `schemas/` — `.proto` files
- `docker-compose.yml`, `Makefile`

—

If you want this README content reflected inside the UI (e.g., an “About” panel with the same diagrams and code excerpts), say the word and I’ll wire it in.
