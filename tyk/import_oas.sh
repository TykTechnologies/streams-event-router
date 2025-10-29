#!/usr/bin/env sh
set -eu
GW=${GW:-http://localhost:8282}
SECRET=${TYK_SECRET:-foo}
INFILE=${1:-/oas/bento-router.oas.json}

echo "Waiting for gateway at $GW ..."
for i in $(seq 1 120); do
  if curl -fsS -H "x-tyk-authorization: $SECRET" "$GW/tyk/apis" >/dev/null 2>&1; then
    echo "Gateway admin API is up."; break; fi; sleep 1;
done

# Convert YAML → JSON when needed; Gateway import expects JSON body
OUTFILE="$INFILE"
CT="application/json"
case "$INFILE" in
  *.yaml|*.yml)
    echo "Converting YAML to JSON for import ..."
    OUTFILE=/tmp/oas.json
    python3 - << 'PY' "$INFILE" "$OUTFILE"
import sys, json
try:
    import yaml  # type: ignore
except Exception:
    sys.stderr.write('PyYAML missing')
    sys.exit(3)
src, dst = sys.argv[1], sys.argv[2]
with open(src, 'r') as f:
    data = yaml.safe_load(f)
with open(dst, 'w') as g:
    json.dump(data, g, separators=(',', ':'))
print('Wrote', dst)
PY
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
