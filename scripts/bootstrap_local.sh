#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

RABBIT_USER=${RABBIT_USER:-guest}
RABBIT_PASS=${RABBIT_PASS:-guest}
RABBIT_API=${RABBIT_API:-http://127.0.0.1:15672/api}
VHOST=${VHOST:-/}
QUEUE_IN=${RABBITMQ_QUEUE:-inbound-queue}
QUEUE_OUT=${RABBITMQ_ROUTING_KEY:-low-priority-queue}
TOPIC=${KAFKA_TOPIC_HIGH:-high-priority-topic}

have() { command -v "$1" >/dev/null 2>&1; }

if have docker && (have docker-compose || docker compose version >/dev/null 2>&1); then
  if docker compose version >/dev/null 2>&1; then
    docker compose up -d rabbitmq redpanda
  else
    docker-compose up -d rabbitmq redpanda
  fi
else
  echo "Docker + compose not available; please start your brokers manually." >&2
  exit 1
fi

# Wait for RabbitMQ
printf "Waiting for RabbitMQ management API"
for i in {1..60}; do
  if curl -fsS -u "$RABBIT_USER:$RABBIT_PASS" "$RABBIT_API/overview" >/dev/null; then echo " ok"; break; fi
  printf "."; sleep 1
  if [ "$i" -eq 60 ]; then echo "\nRabbitMQ API not responding"; exit 1; fi
done

# Declare queues
urlenc_vhost="%2F"
[ "$VHOST" != "/" ] && urlenc_vhost=$(python3 -c 'import urllib.parse,os;print(urllib.parse.quote(os.environ["VHOST"],safe=""))')
for q in "$QUEUE_IN" "$QUEUE_OUT"; do
  echo "Declaring queue: $q"
  curl -fsS -u "$RABBIT_USER:$RABBIT_PASS" -H 'content-type: application/json' \
    -X PUT "$RABBIT_API/queues/$urlenc_vhost/$q" \
    -d '{"durable":true,"auto_delete":false}' >/dev/null
  echo " ok"
done

# Wait for Redpanda
printf "Waiting for Redpanda to be ready"
for i in {1..60}; do
  if curl -fsS http://127.0.0.1:9644/v1/status/ready >/dev/null; then echo " ok"; break; fi
  printf "."; sleep 1
  if [ "$i" -eq 60 ]; then echo "\nRedpanda not ready"; exit 1; fi
done

# Create Kafka topic (via rpk inside container)
RP_CID=$(docker ps --filter name=redpanda --format '{{.ID}}' | head -n1)
if [ -n "$RP_CID" ]; then
  echo "Creating topic: $TOPIC"
  docker exec "$RP_CID" rpk topic create "$TOPIC" --brokers 127.0.0.1:9092 >/dev/null 2>&1 || true
else
  echo "Redpanda container ID not found; skipping topic create"
fi

# Discover ports and write env
bash scripts/discover_ports.sh

. ./.env.benthos

cat <<EOT
Bootstrap complete.
RabbitMQ URL: $RABBITMQ_URL
Kafka bootstrap: $KAFKA_BOOTSTRAP_SERVERS
Queues: in=$QUEUE_IN out=$QUEUE_OUT
Topic: $TOPIC
EOT
