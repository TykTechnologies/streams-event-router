SHELL := /usr/bin/env bash

BENTO ?= $(shell command -v bento 2>/dev/null)
# Default to local config; override with `make CONFIG=...`
CONFIG ?= configs/bento/bento.local.yaml
ENVFILE := .env.benthos

.PHONY: discover lint run run-docker

discover:
	@bash scripts/discover_ports.sh

lint:
ifdef BENTO
	@$(BENTO) -c $(CONFIG) lint
else
	@echo "Using docker to lint..." && \
	docker run --rm -v $$PWD:/work -w /work ghcr.io/warpstreamlabs/bento:latest -c $(CONFIG) lint
endif

run: discover
ifdef BENTO
	@set -euo pipefail; source $(ENVFILE); $(BENTO) -c $(CONFIG)
else
	@$(MAKE) run-docker
endif

run-docker: discover
	@set -euo pipefail; \
	source $(ENVFILE); \
	docker run --rm -it \
	  -e RABBITMQ_URL -e RABBITMQ_QUEUE -e KAFKA_BOOTSTRAP_SERVERS -e KAFKA_TOPIC_HIGH \
	  -e KAFKA_TLS_ENABLED -e KAFKA_SASL_MECHANISM -e KAFKA_SASL_USER -e KAFKA_SASL_PASSWORD \
	  -e AZURE_SB_URL -e AZURE_SB_TARGET -e AZURE_SB_USER -e AZURE_SB_PASSWORD \
	  -e AZURE_EG_KEY -e DEFAULT_HTTP_URL -e AMQP_MODE \
	  -e PROTOBUF_MESSAGE -e PROTOBUF_IMPORTS \
	  -p 4195:4195 \
	  -v $$PWD:/work -w /work \
	  ghcr.io/warpstreamlabs/bento:latest -c $(CONFIG)

.PHONY: compose-up compose-down demo-build demo-run

compose-up:
	@docker compose up -d --build --remove-orphans

compose-down:
	@docker compose down -v --remove-orphans

demo-build:
	@docker build -t event-router-demo:latest ./demo

demo-run:
	@docker run --rm -it -p 8080:8080 \
	  -e KAFKA_BROKER=localhost:9092 -e KAFKA_TOPIC=high-priority-topic -e KAFKA_GROUP=demo-group \
	  -e AMQP_ADDR=amqp://localhost:5672 -e AMQP_SOURCE=low-priority-queue -e AMQP_TARGET=inbound-queue \
	  -e BENTO_URL=http://localhost:4195/inbound \
	  event-router-demo:latest
