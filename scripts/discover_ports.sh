#!/usr/bin/env bash
set -euo pipefail

RABBIT_SERVICE="${RABBIT_SERVICE:-rabbitmq}"
KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
REPD_SERVICE="${REPD_SERVICE:-redpanda}"
OUT_FILE="${OUT_FILE:-.env.benthos}"

discover_port() {
  local svc="$1" port="$2"
  if command -v docker >/dev/null 2>&1; then
    if docker compose version >/dev/null 2>&1; then docker compose port "$svc" "$port" 2>/dev/null | awk '{print $1}' | tail -n1
    elif command -v docker-compose >/dev/null 2>&1; then docker-compose port "$svc" "$port" 2>/dev/null | awk '{print $1}' | tail -n1
    else
      local cid; cid="$(docker ps --filter "name=${svc}" --format '{{.ID}}' | head -n1)"
      [ -n "${cid:-}" ] && docker port "$cid" "$port" 2>/dev/null | awk '{print $1}' | tail -n1 || true
    fi
  fi
}

rabbit_hostport="$(discover_port "${RABBIT_SERVICE}" 5672 || true)"
kafka_hostport="$(discover_port "${KAFKA_SERVICE}" 9092 || true)"
[ -z "${kafka_hostport:-}" ] && kafka_hostport="$(discover_port "${REPD_SERVICE}" 9092 || true)"

rabbit_url="amqp://guest:guest@localhost:5672/"
kafka_bootstrap="localhost:9092"

[ -n "${rabbit_hostport:-}" ] && rabbit_url="amqp://guest:guest@${rabbit_hostport}/"
if [ -n "${kafka_hostport:-}" ]; then
  kafka_bootstrap="${kafka_hostport}"
elif command -v nc >/dev/null 2>&1 && nc -z localhost 29092 2>/dev/null; then
  kafka_bootstrap="localhost:29092"
fi

cat > "${OUT_FILE}" <<EOF
# Generated $(date -u +"%Y-%m-%dT%H:%M:%SZ")
export RABBITMQ_URL=${RABBITMQ_URL:-$rabbit_url}
export KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-$kafka_bootstrap}
export RABBITMQ_QUEUE=\${RABBITMQ_QUEUE:-inbound-queue}
export KAFKA_TOPIC_HIGH=\${KAFKA_TOPIC_HIGH:-high-priority-topic}
export AMQP_MODE=\${AMQP_MODE:-local}
EOF

echo "Wrote ${OUT_FILE}"
grep -E 'RABBITMQ_URL|KAFKA_BOOTSTRAP_SERVERS' "${OUT_FILE}"
