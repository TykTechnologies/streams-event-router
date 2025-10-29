#!/usr/bin/env sh
set -eu
GW=${GW:-http://localhost:8282}
SECRET=${TYK_SECRET:-foo}
FILE=${1:-/oas/bento-router.oas.json}
echo "Waiting for gateway at $GW ..."
for i in $(seq 1 120); do
  if curl -fsS -H "x-tyk-authorization: $SECRET" "$GW/tyk/apis" >/dev/null 2>&1; then
    echo "Gateway admin API is up."; break; fi; sleep 1;
done
echo "POST OAS $FILE to $GW (/tyk/apis/oas?overwrite=true) ..."
curl -fsS -H "x-tyk-authorization: $SECRET" -H 'Content-Type: application/json' \
  -X POST "$GW/tyk/apis/oas?overwrite=true" \
  --data-binary @"$FILE" || { echo "Import failed"; exit 1; }
echo
echo "Reloading gateway ..."
curl -fsS -H "x-tyk-authorization: $SECRET" -X GET "$GW/tyk/reload" || true
echo
