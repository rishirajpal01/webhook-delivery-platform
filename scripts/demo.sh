#!/usr/bin/env bash
# Walks through one send against the local stack (docker compose up --build):
# create an app and an endpoint pointing at the mock receiver, send an event,
# and watch the delivery until it succeeds. The mock receiver fails 30% of
# requests, so some runs show a retry first.
#
# Needs curl and jq. Exits non-zero if the delivery has not succeeded within
# WAIT seconds (default 60), so CI uses it as an end-to-end check.
set -euo pipefail

API=${API:-http://localhost:8080}
KEY=${API_KEY:-dev-api-key-change-me}
SECRET=whsec_ZGVtby1zZWNyZXQtZm9yLWxvY2FsLWRldi1vbmx5ISE= # matches compose.yaml
WAIT=${WAIT:-60}

call() { curl -fsS -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' "$@"; }

app=$(call -X POST "$API/v1/apps" -d '{"name":"demo"}' | jq -r .id)
echo "app:      $app"

ep=$(call -X POST "$API/v1/apps/$app/endpoints" \
  -d "{\"url\":\"http://mockreceiver:9000/hooks\",\"event_types\":[\"order.paid\"],\"secret\":\"$SECRET\"}" | jq -r .id)
echo "endpoint: $ep"

msg=$(call -X POST "$API/v1/apps/$app/messages" -H "Idempotency-Key: demo-$(date +%s%N)" \
  -d '{"event_type":"order.paid","payload":{"order_id":"981","amount":4999}}' | jq -r .id)
echo "message:  $msg"

for _ in $(seq "$WAIT"); do
  delivery=$(call "$API/v1/apps/$app/messages/$msg/deliveries" | jq '.data[0]')
  state=$(jq -r .state <<<"$delivery")
  echo "delivery: $state after $(jq -r .attempt_count <<<"$delivery") attempt(s)"
  if [ "$state" = succeeded ]; then
    echo "attempt log:"
    call "$API/v1/apps/$app/deliveries/$(jq -r .id <<<"$delivery")/attempts" \
      | jq -c '.data[] | {attempted_at, status_code, latency_ms}'
    exit 0
  fi
  sleep 1
done
echo "delivery did not succeed within ${WAIT}s" >&2
exit 1
