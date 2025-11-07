#!/usr/bin/env sh
set -eu
DASH=${DASH:-http://tyk-dashboard:3000}
ADMIN_SECRET=${ADMIN_SECRET:-12345}
INFILE=${1:-/oas/streams-router.oas.yaml}
EXTRA=${EXTRA_OAS:-/oas/diag.oas.yaml}

echo "Waiting for Dashboard at $DASH ..."
for i in $(seq 1 120); do
  if curl -fsS "$DASH/hello" >/dev/null 2>&1; then
    echo "Dashboard API is up."; break; fi; sleep 1;
done

# Ensure yq is available (used for YAML→JSON and JSON filtering)
YQ=/tmp/yq
if [ ! -x "$YQ" ]; then
  echo "Fetching yq (YAML/JSON tool) ..."
  curl -fsSL -o "$YQ" https://github.com/mikefarah/yq/releases/download/v4.44.2/yq_linux_amd64
  chmod +x "$YQ"
fi

# Create default organization
echo "Creating default organization..."
ORG_RESPONSE=$(curl -fsS -H "admin-auth: $ADMIN_SECRET" -H "Content-Type: application/json" \
  "$DASH/admin/organisations" \
  -d '{"owner_name":"Default Admin","cname_enabled":true,"cname":""}' || echo '{}')

ORG_ID=$(echo "$ORG_RESPONSE" | "$YQ" -p=json -o=json '.Meta' 2>/dev/null || echo "")
if [ -z "$ORG_ID" ]; then
  echo "Failed to create organization or organization already exists, attempting to continue..."
  # Try to get first existing org
  ORGS=$(curl -fsS -H "admin-auth: $ADMIN_SECRET" "$DASH/admin/organisations" || echo '{"organisations":[]}')
  ORG_ID=$(echo "$ORGS" | "$YQ" -p=json -o=json '.organisations[0].id' 2>/dev/null || echo "")
fi
echo "Organization ID: $ORG_ID"

# Create initial user
echo "Creating initial user..."
USER_RESPONSE=$(curl -fsS -H "admin-auth: $ADMIN_SECRET" -H "Content-Type: application/json" \
  "$DASH/admin/users" \
  -d '{"org_id":'$ORG_ID',"first_name":"Admin","last_name":"User","email_address":"dev@tyk.io","active":true,"user_permissions":{"IsAdmin":"admin"}}' || echo '{}')

USER_ID=$(echo "$USER_RESPONSE" | "$YQ" -p=json '.Meta.id' 2>/dev/null || echo "")
USER_ACCESS_KEY=$(echo "$USER_RESPONSE" | "$YQ" -p=json '.Meta.access_key' 2>/dev/null || echo "")
echo "User ID: $USER_ID"
echo "User Access Key: $USER_ACCESS_KEY"

# Reset user password via Dashboard API
if [ -n "$USER_ID" ] && [ -n "$USER_ACCESS_KEY" ]; then
  echo "Resetting user password..."
  RESET_RESPONSE=$(curl -fsS -H "authorization: $USER_ACCESS_KEY" -H "Content-Type: application/json" \
    "$DASH/api/users/$USER_ID/actions/reset" \
    -d '{"new_password":"topsecret"}' || echo '{}')

    echo "Password reset completed"
else
  echo "Warning: Could not extract user ID or access key, skipping password reset"
fi
echo "User setup complete. Auth token ready."

# If a YAML file is provided, convert it to JSON using a tiny yq binary fetched via curl.
OUTFILE="$INFILE"
case "$INFILE" in
  *.yaml|*.yml)
    echo "Converting YAML to JSON ..."
    OUTFILE=/tmp/oas.json
    "$YQ" -o=json "$INFILE" > "$OUTFILE"
    ;;
esac

echo "POST OAS $OUTFILE to Dashboard (/api/apis/oas) ..."
curl -fsS -H "authorization: $USER_ACCESS_KEY" -H "Content-Type: application/json" \
  "$DASH/api/apis/oas" \
  --data-binary @"$OUTFILE" || { echo "Import failed"; exit 1; }
echo
echo "API imported successfully"
echo

# Import extra OAS if present (e.g., Diagnostics API)
if [ -f "$EXTRA" ]; then
  echo "Importing extra OAS: $EXTRA ..."
  OUT2=/tmp/extra.json
  "$YQ" -o=json "$EXTRA" > "$OUT2"
  curl -fsS -H "authorization: $USER_ACCESS_KEY" -H "Content-Type: application/json" \
    "$DASH/api/apis/oas" \
    --data-binary @"$OUT2" || { echo "Extra import failed"; exit 1; }
  echo
  echo "Extra API imported successfully"
  echo
fi