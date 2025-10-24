#!/bin/sh
set -e

KAFKA_HOST=${KAFKA_HOST:-labs-kafka}
KAFKA_PORT=${KAFKA_PORT:-9092}
RABBIT_HOST=${RABBIT_HOST:-rabbitmq}
RABBIT_PORT=${RABBIT_PORT:-5672}
BENTO_CONFIG=${BENTO_CONFIG:-/work/bento.local.yaml}

echo "[streams] Waiting for Kafka at ${KAFKA_HOST}:${KAFKA_PORT}"
while ! nc -z ${KAFKA_HOST} ${KAFKA_PORT} >/dev/null 2>&1; do sleep 1; done
echo "[streams] Kafka up"

echo "[streams] Waiting for RabbitMQ at ${RABBIT_HOST}:${RABBIT_PORT}"
while ! nc -z ${RABBIT_HOST} ${RABBIT_PORT} >/dev/null 2>&1; do sleep 1; done
echo "[streams] RabbitMQ up"

exec bento -c "${BENTO_CONFIG}"
