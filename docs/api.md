# API

The HTTP API is described in [openapi.json](openapi.json) (OpenAPI 3.1),
which the running service also serves at `GET /openapi.json`. That file is
the source of truth: CI fails if the server or the Go SDK disagree with it.
The [Go SDK and CLI](sdk-and-cli.md) wrap every route below.

All `/v1` routes need `Authorization: Bearer $API_KEY`. Errors are
[RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) `application/problem+json`.
Every create (apps, endpoints, messages) honours an `Idempotency-Key` header:
a repeat with the same key returns the first object with `200` and
`Idempotent-Replayed: true` instead of creating another. Lists take
`?limit=` (1 to 100, default 50) and `?cursor=`, and return `next_cursor`
while more remain.

| Method and path | Purpose |
| --- | --- |
| `POST /v1/apps` | Create an application `{"name"}` |
| `GET /v1/apps/{app}` | Read an application |
| `POST /v1/apps/{app}/endpoints` | Create an endpoint `{"url", "event_types"?, "secret"?, "max_in_flight"?, "rate_limit"?}`; returns the signing secret once |
| `GET /v1/apps/{app}/endpoints` | List endpoints |
| `GET`, `PATCH`, `DELETE /v1/apps/{app}/endpoints/{ep}` | Read, change (`url`, `event_types`, `status`: active or paused, `max_in_flight`, `rate_limit`) or delete an endpoint. Setting `active` also re-enables a disabled endpoint and clears its failure history |
| `POST /v1/apps/{app}/endpoints/{ep}/secret/rotate` | New signing secret `{"secret"?}`, returned once; the old one keeps signing for 24 h |
| `POST /v1/apps/{app}/endpoints/{ep}/enable` | Re-enable a disabled endpoint and clear its failure history (same as `PATCH` with `"status": "active"`) |
| `POST /v1/apps/{app}/endpoints/{ep}/replay?since=<RFC 3339>` | Redeliver every `dead` delivery created since then (a `{"since"}` body works too); returns `{"replayed": n}` |
| `POST /v1/apps/{app}/messages` | Send an event `{"event_type", "payload"}` |
| `GET /v1/apps/{app}/messages/{msg}/deliveries` | Where a message went and the state of each delivery |
| `GET /v1/apps/{app}/deliveries/{del}/attempts` | Every attempt: status, latency, error, first 4 KB of the response |
| `POST /v1/apps/{app}/deliveries/{del}/replay` | Send a `dead` or `succeeded` delivery again (409 while it is still pending) |
| `GET /healthz`, `/readyz`, `/metrics`, `/openapi.json` | Liveness, readiness (Postgres reachable), Prometheus metrics, this API's description (no key needed) |

```sh
curl -X POST localhost:8080/v1/apps/$APP/messages \
  -H "Authorization: Bearer dev-api-key-change-me" \
  -H "Idempotency-Key: order-981-paid" \
  -d '{"event_type":"order.paid","payload":{"order_id":"981","amount":4999}}'
# 202 {"id":"msg_…","deliveries":1}   (a repeat with the same key: 200, same id)
```

An endpoint with no `event_types` receives every event type the operator
sends. What a receiver gets is described in [receiving.md](receiving.md).
