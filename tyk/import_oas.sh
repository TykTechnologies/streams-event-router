#!/usr/bin/env sh
set -eu
GW=${GW:-http://localhost:8282}
SECRET=${TYK_SECRET:-foo}
INFILE=${1:-/oas/streams-router.oas.yaml}
EXTRA=${EXTRA_OAS:-/oas/diag.oas.yaml}

echo "Waiting for gateway at $GW ..."
for i in $(seq 1 120); do
  if curl -fsS -H "x-tyk-authorization: $SECRET" "$GW/tyk/apis" >/dev/null 2>&1; then
    echo "Gateway admin API is up."; break; fi; sleep 1;
done

# Ensure yq is available (used for YAML→JSON and JSON filtering)
YQ=/tmp/yq
if [ ! -x "$YQ" ]; then
  echo "Fetching yq (YAML/JSON tool) ..."
  curl -fsSL -o "$YQ" https://github.com/mikefarah/yq/releases/download/v4.44.2/yq_linux_amd64
  chmod +x "$YQ"
fi

# Delete any prior APIs by name or listen path to avoid collisions
API_NAME="Event Router (Streams)"
echo "Cleaning prior APIs named: $API_NAME or with listen_path /streams-api/ ..."
ALL=$(curl -fsS -H "x-tyk-authorization: $SECRET" "$GW/tyk/apis" || echo '[]')
IDS_BY_NAME=$(echo "$ALL" | "$YQ" -p=json -o=json '.[] | select(.name == strenv(API_NAME)) | .api_id' 2>/dev/null || true)
IDS_BY_PATH=$(echo "$ALL" | "$YQ" -p=json -o=json '.[] | select(.api_definition.proxy.listen_path | startswith("/streams-api/")) | .api_id' 2>/dev/null || true)
for id in $IDS_BY_NAME $IDS_BY_PATH; do
  echo "Deleting API id=$id"
  curl -fsS -H "x-tyk-authorization: $SECRET" -X DELETE "$GW/tyk/apis/oas/$id" >/dev/null || true
done

# If a YAML file is provided, convert it to JSON using a tiny yq binary fetched via curl.
OUTFILE="$INFILE"
case "$INFILE" in
  *.yaml|*.yml)
    echo "Converting YAML to JSON ..."
    OUTFILE=/tmp/oas.json
    "$YQ" -o=json "$INFILE" > "$OUTFILE"
    ;;
esac

echo "POST OAS $OUTFILE to $GW (/tyk/apis/oas?overwrite=true) ..."
curl -fsS -H "x-tyk-authorization: $SECRET" -H "Content-Type: application/json" \
  -X POST "$GW/tyk/apis/oas?overwrite=true" \
  --data-binary @"$OUTFILE" || { echo "Import failed"; exit 1; }
echo
echo "Reloading gateway ..."
curl -fsS -H "x-tyk-authorization: $SECRET" -X GET "$GW/tyk/reload" || true
echo

# Import extra OAS if present (e.g., Diagnostics API)
if [ -f "$EXTRA" ]; then
  echo "Importing extra OAS: $EXTRA ..."
  OUT2=/tmp/extra.json
  "$YQ" -o=json "$EXTRA" > "$OUT2"
  curl -fsS -H "x-tyk-authorization: $SECRET" -H "Content-Type: application/json" \
    -X POST "$GW/tyk/apis/oas?overwrite=true" \
    --data-binary @"$OUT2" || { echo "Extra import failed"; exit 1; }
  echo
  echo "Reloading gateway ..."
  curl -fsS -H "x-tyk-authorization: $SECRET" -X GET "$GW/tyk/reload" || true
  echo
fi
