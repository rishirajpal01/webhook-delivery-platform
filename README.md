# webhook-delivery-platform

A self-hostable webhook delivery service: your product sends an event once,
and the service delivers it to every subscribed endpoint, retrying with
backoff, signing each request with [Standard Webhooks](https://www.standardwebhooks.com/),
and recording every attempt.

One Go binary plus Postgres. No Kafka, Redis or Kubernetes required.

> **Status: phase 1 (core delivery, MVP).** Sending, fan-out, retries,
> signing, the attempt log and the SSRF guard work. Replay, circuit breaking
> and per-endpoint fairness come in phases 2 and 3.

## Guarantees

- **At-least-once, unordered.** Every request carries a stable `webhook-id`
  (the message id); receivers drop ids they have already processed.
- **No lost messages across crashes.** A message and its deliveries are
  written in one transaction. Dispatchers claim deliveries with
  `FOR UPDATE SKIP LOCKED` and a 30 s lease; if one dies mid-send, the lease
  expires and another dispatcher sends again. `TestCrashLosesNothing` checks
  this by `kill -9`-ing the dispatcher repeatedly (100 000 messages, 8 kills:
  none lost, 21 duplicates).

## Run it

```sh
docker compose up --build
scripts/demo.sh            # needs curl and jq
```

The demo creates an app and an endpoint pointing at the mock receiver, sends
an `order.paid` event and follows the delivery. The mock receiver verifies
every signature and fails 30% of requests, so some runs retry first:

```
delivery: succeeded after 3 attempt(s)
attempt log:
{"attempted_at":"…:27Z","status_code":500,"latency_ms":0}
{"attempted_at":"…:33Z","status_code":500,"latency_ms":5}
{"attempted_at":"…:07Z","status_code":200,"latency_ms":0}
```

## API

All `/v1` routes need `Authorization: Bearer $API_KEY`. Errors are
[RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) `application/problem+json`.

| Method and path | Purpose |
| --- | --- |
| `POST /v1/apps` | Create an application `{"name"}` |
| `GET /v1/apps/{app}` | Read an application |
| `POST /v1/apps/{app}/endpoints` | Create an endpoint `{"url", "event_types"?, "secret"?}`; returns the signing secret once |
| `GET /v1/apps/{app}/endpoints?limit=&cursor=` | List endpoints |
| `GET`, `PATCH`, `DELETE /v1/apps/{app}/endpoints/{ep}` | Read, change (`url`, `event_types`, `status`: active or paused) or delete an endpoint |
| `POST /v1/apps/{app}/messages` | Send an event `{"event_type", "payload"}`; honours `Idempotency-Key` |
| `GET /v1/apps/{app}/messages/{msg}/deliveries` | Where a message went and the state of each delivery |
| `GET /v1/apps/{app}/deliveries/{del}/attempts` | Every attempt: status, latency, error, first 4 KB of the response |
| `GET /healthz`, `/readyz`, `/metrics` | Liveness, readiness (Postgres reachable), Prometheus metrics |

```sh
curl -X POST localhost:8080/v1/apps/$APP/messages \
  -H "Authorization: Bearer dev-api-key-change-me" \
  -H "Idempotency-Key: order-981-paid" \
  -d '{"event_type":"order.paid","payload":{"order_id":"981","amount":4999}}'
# 202 {"id":"msg_…","deliveries":1}   (a repeat with the same key: 200, same id)
```

An endpoint with no `event_types` receives every event type.

## Delivery behaviour

- **Retries** on network errors, timeouts, redirects and any non-2xx, after
  5 s, 30 s, 2 m, 10 m, 30 m, 1 h, 3 h, 6 h and 12 h (each ±25% jitter), then
  the delivery is `dead`. Ten attempts over about 23 hours.
- **Timeouts:** 15 s per request; redirects are not followed.
- **SSRF guard:** connections to loopback, private, link-local (including
  cloud metadata at 169.254.169.254) and reserved addresses are refused. The
  check runs after DNS resolution, so DNS rebinding cannot get around it.
- **Signing:** `webhook-id`, `webhook-timestamp` and
  `webhook-signature: v1,<base64 HMAC-SHA256 of id.timestamp.body>`, so any
  Standard Webhooks library can verify requests.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | none | Postgres connection string (required) |
| `ROLE` | `all` | `api`, `dispatcher`, or `all` for both in one process |
| `API_KEY` | none | Bearer token for `/v1`, at least 16 characters (required for `api` and `all`) |
| `LISTEN_ADDR` | `:8080` | HTTP address; dispatchers serve only health and metrics |
| `DISPATCHER_CONCURRENCY` | `16` | Deliveries one process sends at once |
| `DISPATCHER_LEASE` | `30s` | How long a claimed delivery stays reserved; keep it above the 15 s request timeout |
| `ALLOW_PRIVATE_NETWORKS` | `false` | Turns the SSRF guard off. Local development only |
| `SHUTDOWN_TIMEOUT` | `10s` | Time in-flight HTTP requests get on shutdown |

Any number of `api` and `dispatcher` processes can run against one Postgres.

## Metrics

| Metric | Meaning |
| --- | --- |
| `webhookd_messages_created_total` | Messages accepted (idempotent repeats excluded) |
| `webhookd_delivery_attempts_total{outcome}` | Attempts by outcome: `succeeded`, `retry`, `dead` |
| `webhookd_delivery_attempt_duration_seconds` | Request duration |
| `webhookd_delivery_lag_seconds` | Time from a delivery becoming due to being claimed |
| `webhookd_deliveries_in_flight` | Sends in progress in this process |

## Develop

Needs Go 1.25+. Postgres tests run when `DATABASE_URL` is set and skip
otherwise.

```sh
make up      # start the stack (Postgres on localhost:5432)
make test    # go test -race ./...  (includes the crash test at 2 000 messages)
make lint    # gofmt, go vet, staticcheck
make down

# The full crash test from the design doc:
CRASH_TEST_MESSAGES=100000 CRASH_TEST_KILLS=8 go test -run Crash -timeout 10m ./cmd/webhookd
```

## Layout

```
cmd/webhookd         the service (api and dispatcher roles)
cmd/mockreceiver     a receiver for demos and tests (-fail-rate, -latency, -secret)
internal/api         HTTP routes, auth, validation
internal/dispatcher  claim, send, retry
internal/postgres    connection, migrations, queries
internal/signing     Standard Webhooks sign and verify
internal/ssrf        dialer that refuses non-public addresses
internal/config      environment config
scripts/demo.sh      end-to-end walkthrough (also run in CI)
```

## Roadmap

| Phase | Ships |
| ----- | ----- |
| 0 · Foundations ✓ | Layout, CI, compose, migrations, mock receiver |
| 1 · Core delivery ✓ | Apps, endpoints, send with idempotency, fan-out, dispatcher with leases, retries, signing, attempt log, SSRF guard, metrics |
| 2 · Reliability | Replay, circuit breaker and auto-disable, `Retry-After`, secret rotation, graceful drain |
| 3 · Fairness and scale | Per-endpoint concurrency and rate limits, multiple dispatchers, partitioned attempts, load generator |
| 4 · Developer experience | OpenAPI spec, Go SDK, `Verify()` helper, CLI |
| 5 · Launch | Benchmarks in this README, write-up, v1.0 |

## License

Apache-2.0
