#!/usr/bin/env sh
set -eu
GW=${GW:-http://localhost:8282}
SECRET=${TYK_SECRET:-foo}
INFILE=${1:-/oas/bento-router.oas.yaml}

echo "Waiting for gateway at $GW ..."
for i in $(seq 1 120); do
  if curl -fsS -H "x-tyk-authorization: $SECRET" "$GW/tyk/apis" >/dev/null 2>&1; then
    echo "Gateway admin API is up."; break; fi; sleep 1;
done

# If a YAML file is provided, convert it to JSON using a tiny yq binary fetched via curl.
OUTFILE="$INFILE"
case "$INFILE" in
  *.yaml|*.yml)
    echo "Fetching yq (YAML→JSON) ..."
    YQ=/tmp/yq
    if [ ! -x "$YQ" ]; then
      curl -fsSL -o "$YQ" https://github.com/mikefarah/yq/releases/download/v4.44.2/yq_linux_amd64
      chmod +x "$YQ"
    fi
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
