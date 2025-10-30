SHELL := /usr/bin/env bash

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
	  -e TYK_STREAMS_URL=http://localhost:18282/streams-api/event \
	  event-router-demo:latest
