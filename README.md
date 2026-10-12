# webhook-delivery-platform

A self-hostable webhook delivery service: your product sends an event once,
and the service delivers it to every subscribed endpoint, retrying with
backoff, signing each request with [Standard Webhooks](https://www.standardwebhooks.com/),
and recording every attempt.

One Go binary plus Postgres. No Kafka, Redis or Kubernetes required.

> **Status: phase 3 (fairness and scale).** Sending, fan-out, retries,
> signing, the attempt log, the SSRF guard, replay, circuit breaking and
> secret rotation work, plus per-endpoint concurrency and rate limits that
> hold across any number of dispatcher replicas, a partitioned attempt log
> with retention, and a load generator. The OpenAPI spec, Go SDK and CLI come
> in phase 4.

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
| `POST /v1/apps/{app}/endpoints` | Create an endpoint `{"url", "event_types"?, "secret"?, "max_in_flight"?, "rate_limit"?}`; returns the signing secret once |
| `GET /v1/apps/{app}/endpoints?limit=&cursor=` | List endpoints |
| `GET`, `PATCH`, `DELETE /v1/apps/{app}/endpoints/{ep}` | Read, change (`url`, `event_types`, `status`: active or paused, `max_in_flight`, `rate_limit`) or delete an endpoint. Setting `active` also re-enables a disabled endpoint and clears its failure history |
| `POST /v1/apps/{app}/endpoints/{ep}/secret/rotate` | New signing secret `{"secret"?}`, returned once; the old one keeps signing for 24 h |
| `POST /v1/apps/{app}/endpoints/{ep}/replay` | Redeliver every `dead` delivery created since `{"since": "<RFC 3339>"}`; returns `{"replayed": n}` |
| `POST /v1/apps/{app}/messages` | Send an event `{"event_type", "payload"}`; honours `Idempotency-Key` |
| `GET /v1/apps/{app}/messages/{msg}/deliveries` | Where a message went and the state of each delivery |
| `GET /v1/apps/{app}/deliveries/{del}/attempts` | Every attempt: status, latency, error, first 4 KB of the response |
| `POST /v1/apps/{app}/deliveries/{del}/replay` | Send a `dead` or `succeeded` delivery again (409 while it is still pending) |
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

- **Fairness:** each endpoint gets at most `max_in_flight` concurrent sends
  (default 4, up to 100) and, if `rate_limit` is set, that many requests per
  second (bursts of up to one second's worth). Dispatchers serve endpoints
  round-robin and skip any endpoint at its limit, so one slow or backlogged
  endpoint cannot hold up the rest. The limits live in Postgres and are
  enforced inside the claim, so they hold across all replicas.
  `TestNoisyNeighbour` checks this: an endpoint that takes 2 s and then
  fails, with 500 deliveries queued, leaves nine healthy endpoints at a p99
  lag of about 25 ms (about 2 s with the cap lifted).

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
- **`Retry-After`:** on a 429 or 503 the next attempt waits at least as long
  as the receiver asked (seconds or an HTTP date, capped at 24 h).
- **Circuit breaker:** after 10 failures in a row an endpoint's deliveries
  stop being sent. After 1 minute one probe delivery is tried; if it fails
  the wait doubles (up to 1 h), if it succeeds everything queued flows
  again. Other endpoints are unaffected.
- **Auto-disable:** an endpoint that has failed continuously for 5 days is
  set to `disabled` (`disabled_at` shows when). Its deliveries wait; set
  `status` back to `active` to resume them.
- **Replay:** sends a delivery again with the same `webhook-id`, so receivers
  that already processed it can still drop it. It starts a fresh retry
  schedule.
- **Secret rotation:** for 24 h after a rotation each request carries two
  signatures, one per secret, so receivers can switch at their own pace.
- **Graceful drain:** on SIGTERM a dispatcher stops claiming, finishes the
  sends it started and records them before exiting.

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
| `ATTEMPT_RETENTION_DAYS` | `30` | Days of the attempt log to keep; older days are dropped whole. `0` keeps everything |

Any number of `api` and `dispatcher` processes can run against one Postgres.
With compose: `docker compose --profile scale up --build --scale dispatcher=3`.

The attempt log is partitioned by day (UTC). Dispatchers create partitions
for the next 7 days at start and hourly, and drop the ones past retention.

## Metrics

| Metric | Meaning |
| --- | --- |
| `webhookd_messages_created_total` | Messages accepted (idempotent repeats excluded) |
| `webhookd_delivery_attempts_total{outcome}` | Attempts by outcome: `succeeded`, `retry`, `dead` |
| `webhookd_delivery_attempt_duration_seconds` | Request duration |
| `webhookd_delivery_lag_seconds` | Time from a delivery becoming due to being claimed |
| `webhookd_deliveries_in_flight` | Sends in progress in this process |
| `webhookd_endpoint_circuit_trips_total` | Times an endpoint's circuit breaker opened |
| `webhookd_endpoints_disabled_total` | Endpoints disabled after failing continuously |

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
cmd/loadgen          load generator with its own receiver (throughput and lag)
internal/dispatcher  claim, send, retry, circuit breaker
internal/postgres    connection, migrations, queries
internal/signing     Standard Webhooks sign and verify
internal/ssrf        dialer that refuses non-public addresses
internal/config      environment config
scripts/demo.sh      end-to-end walkthrough (also run in CI)
scripts/bench.sh     benchmark with 1, 2 and 4 dispatcher replicas
```

## Roadmap

| Phase | Ships |
| ----- | ----- |
| 0 · Foundations ✓ | Layout, CI, compose, migrations, mock receiver |
| 1 · Core delivery ✓ | Apps, endpoints, send with idempotency, fan-out, dispatcher with leases, retries, signing, attempt log, SSRF guard, metrics |
| 2 · Reliability ✓ | Replay, circuit breaker and auto-disable, `Retry-After`, secret rotation, graceful drain |
| 3 · Fairness and scale ✓ | Per-endpoint concurrency and rate limits, multiple dispatchers, partitioned attempts, load generator |
| 4 · Developer experience | OpenAPI spec, Go SDK, `Verify()` helper, CLI |
| 5 · Launch | Benchmarks in this README, write-up, v1.0 |

## License

Apache-2.0
