# webhook-delivery-platform

A self-hostable webhook delivery service: your product sends an event once,
and the service delivers it to every subscribed endpoint, retrying with
backoff, signing each request with [Standard Webhooks](https://www.standardwebhooks.com/),
and recording every attempt.

One Go binary plus Postgres. No Kafka, Redis or Kubernetes required.

> **Status: phase 4 (developer experience).** Everything in the design doc
> works: sending, fan-out, retries, signing, the attempt log, the SSRF guard,
> replay, circuit breaking, secret rotation, per-endpoint concurrency and rate
> limits across any number of dispatchers, a partitioned attempt log, an
> OpenAPI spec, a Go SDK with `VerifyRequest` for receivers, and a CLI. The
> v1.0 launch is phase 5.

## Guarantees

- **At-least-once, unordered.** Every request carries a stable `webhook-id`
  (the message id); receivers drop ids they have already processed.
- **No lost messages across crashes.** A message and its deliveries are
  written in one transaction. Dispatchers claim deliveries with
  `FOR UPDATE SKIP LOCKED` and a 30 s lease; if one dies mid-send, the lease
  expires and another dispatcher sends again. `TestCrashLosesNothing` checks
  this by `kill -9`-ing the dispatcher repeatedly (100 000 messages, 8 kills:
  none lost, 21 duplicates).

## Quickstart

Needs Docker. This starts Postgres, the service and a mock receiver that
checks the signature of every request, then sends one event with the
bundled `webhookctl` CLI and waits for it to arrive:

<!-- quickstart: CI runs this block as is (scripts/quickstart.sh) -->
```sh
docker compose up --build -d --wait
ctl() { docker compose exec -T webhookd webhookctl "$@"; }

# An app, and an endpoint pointing at the mock receiver, which knows this secret
APP=$(ctl -o '{{.id}}' apps create demo)
ctl endpoints create "$APP" http://mockreceiver:9000/hooks -events order.paid \
  -secret whsec_ZGVtby1zZWNyZXQtZm9yLWxvY2FsLWRldi1vbmx5ISE=

# Send an event and wait until it is delivered (the mock fails 30% of
# requests, so you may see a retry first)
MSG=$(ctl -o '{{.id}}' send "$APP" order.paid '{"order_id":"981","amount":4999}')
ctl deliveries "$APP" "$MSG" -wait 5m

# Every attempt, and the receiver's verdict on each signature
ctl attempts "$APP" "$(ctl -o '{{(index .data 0).id}}' deliveries "$APP" "$MSG")"
docker compose logs mockreceiver | grep "$MSG"
```

The receiver logs `"signature":"ok"` for each attempt. `docker compose down -v`
stops everything. To receive webhooks in your own code, see
[docs/receiving.md](docs/receiving.md).

## Docs

| Page | For |
| --- | --- |
| [docs/receiving.md](docs/receiving.md) | Receivers: the request format, verifying signatures, answering, retries |
| [docs/api.md](docs/api.md) and [docs/openapi.json](docs/openapi.json) | The HTTP API: routes, idempotency, pagination, errors |
| [docs/sdk-and-cli.md](docs/sdk-and-cli.md) | The Go SDK and `webhookctl` |
| [docs/delivery.md](docs/delivery.md) | How delivery works: fairness, retries, circuit breaker, replay, rotation |
| [docs/operations.md](docs/operations.md) | Running it: configuration, scaling, retention, metrics |

## Benchmarks

`scripts/bench.sh` runs `cmd/loadgen` against 1, 2 and 4 dispatcher
replicas. Each message fans out to 20 endpoints (`max_in_flight` 4) on a
receiver that takes 20 ms to answer, and each dispatcher sends 16 at once.
Lag is end to end: from the send call starting to the delivery arriving.

Measured on a 4-CPU Linux VM with Postgres, the API, the dispatchers and the
load generator all on the same machine, so the 4-replica run is short of
CPU. Every run delivered every message, with no duplicates.

| Dispatchers | Burst throughput (60 000 deliveries) | Steady lag at 400 deliveries/s, p50 / p99 |
| --- | --- | --- |
| 1 | 576 deliveries/s | 21 ms / 195 ms |
| 2 | 1 045 deliveries/s | 57 ms / 253 ms |
| 4 | 1 458 deliveries/s | 72 ms / 247 ms |

Steady lag is mostly the 250 ms poll interval: an idle dispatcher checks
for new work four times a second.

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

# The replica benchmark (needs psql and a Postgres it can create databases on):
ADMIN_URL=postgres://postgres@localhost:5432/postgres scripts/bench.sh
```

## Layout

```
cmd/webhookd         the service (api and dispatcher roles)
cmd/webhookctl       the CLI, built on the SDK
cmd/mockreceiver     a receiver for demos and tests (-fail-rate, -latency, -secret)
cmd/loadgen          load generator with its own receiver (throughput and lag)
webhooks             the Go SDK: API client and VerifyRequest for receivers
examples/receiver    a minimal receiver using the SDK
docs                 docs pages and openapi.json, the API's source of truth
internal/api         HTTP routes, auth, validation
internal/apispec     reads openapi.json for the contract tests
internal/dispatcher  claim, send, retry, circuit breaker
internal/postgres    connection, migrations, queries
internal/signing     Standard Webhooks sign and verify
internal/ssrf        dialer that refuses non-public addresses
internal/config      environment config
scripts/quickstart.sh runs the README quickstart as written (CI does too)
scripts/bench.sh     benchmark with 1, 2 and 4 dispatcher replicas
```

## Roadmap

| Phase | Ships |
| ----- | ----- |
| 0 · Foundations ✓ | Layout, CI, compose, migrations, mock receiver |
| 1 · Core delivery ✓ | Apps, endpoints, send with idempotency, fan-out, dispatcher with leases, retries, signing, attempt log, SSRF guard, metrics |
| 2 · Reliability ✓ | Replay, circuit breaker and auto-disable, `Retry-After`, secret rotation, graceful drain |
| 3 · Fairness and scale ✓ | Per-endpoint concurrency and rate limits, multiple dispatchers, partitioned attempts, load generator |
| 4 · Developer experience ✓ | OpenAPI spec, Go SDK, `VerifyRequest()` for receivers, CLI, docs |
| 5 · Launch | Benchmarks in this README, write-up, v1.0 |

## License

Apache-2.0
