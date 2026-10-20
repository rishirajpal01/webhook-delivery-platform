# webhook-delivery-platform

A self-hostable webhook delivery service: your product sends an event once,
and the service delivers it to every subscribed endpoint, retrying with
backoff, signing each request with [Standard Webhooks](https://www.standardwebhooks.com/),
and recording every attempt.

One Go binary plus Postgres. No Kafka, Redis or Kubernetes required.

<a href="docs/architecture/README.md">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/architecture/architecture-dark.png" />
    <img src="docs/architecture/architecture-light.png" alt="Architecture: your product sends to webhookd's API, which writes the message and its deliveries to Postgres in one transaction; dispatchers claim deliveries with SKIP LOCKED and a lease, record each attempt in a day-partitioned log, and send signed requests through an SSRF guard to receivers." />
  </picture>
</a>

The interactive version, with every box linked to its source, is
[docs/architecture/index.html](docs/architecture/index.html).

> **Status: v1.0.** Everything in the design doc
> works: sending, fan-out, retries, signing, the attempt log, the SSRF guard,
> replay, circuit breaking, secret rotation, per-endpoint concurrency and rate
> limits across any number of dispatchers, a partitioned attempt log, an
> OpenAPI spec, a Go SDK with `VerifyRequest` for receivers, and a CLI. How it
> works and why: [docs/writeup.md](docs/writeup.md).

## System Overview

Self-hostable webhook delivery service. One Go binary, one Postgres database. No Kafka, Redis, or queue daemon needed.
Delivers messages at-least-once, unordered. Every message carries permanent `webhook-id`. Receivers deduplicate on id.

```mermaid
flowchart LR
    Producer["Your Product / Producer"] -->|"POST /v1/apps/{app}/messages"| API["webhookd (API role)"]
    API -->|"1 DB transaction"| PG[("Postgres Queue<br/>messages + deliveries")]
    Dispatcher["webhookd (Dispatcher role)<br/>1..N replicas"] -->|"Claim: SKIP LOCKED + lease"| PG
    Dispatcher -->|"Dial check"| SSRF["SSRF Guard<br/>blocks non-public IP"]
    SSRF -->|"POST signed webhook"| Receiver["Customer Receiver Endpoint"]
    Dispatcher -->|"Record attempt & state"| PGLog[("Postgres Attempt Log<br/>Day-partitioned")]
```

### Binary Roles & Process Model

Single binary [`cmd/webhookd`](cmd/webhookd). Behavior controlled by `ROLE` environment variable:

- `api`: Serves REST API routes under `/v1` with bearer token auth ([`internal/api/api.go`](internal/api/api.go)).
- `dispatcher`: Claims due deliveries from Postgres, signs HTTP requests, dispatches them ([`internal/dispatcher/dispatcher.go`](internal/dispatcher/dispatcher.go)).
- `all` (default): Runs API and dispatcher concurrently in same process.

All roles expose `/healthz`, `/readyz`, `/metrics` (Prometheus), and `/openapi.json`.

### Data Model

Postgres tables act as state store and persistent work queue ([`internal/postgres/migrations/`](internal/postgres/migrations/)).

```mermaid
erDiagram
    applications ||--o{ endpoints : owns
    applications ||--o{ messages : contains
    messages ||--o{ deliveries : "fans out into"
    endpoints ||--o{ deliveries : receives
    deliveries ||--o{ attempts : logs
```

- **`applications`**: Tenant boundaries (`app_...`).
- **`endpoints`**: Target URLs, signing secrets, status (`active`, `paused`, `disabled`), subscribed `event_types`, concurrency cap (`max_in_flight`), rate limits (`rate_limit`, `rate_tat`).
- **`messages`**: Event payload, event type, optional idempotency key.
- **`deliveries`**: Individual queue item representing one message going to one endpoint. States: `pending`, `in_flight`, `succeeded`, `dead`.
- **`attempts`**: Audit log of every HTTP call. Range-partitioned by UTC day.

### Ingestion & Fanout Pipeline

```mermaid
sequenceDiagram
    autonumber
    actor P as Producer (Product)
    participant A as webhookd API
    participant DB as Postgres
    P->>A: POST /v1/apps/{app}/messages (payload, event_type, Idempotency-Key)
    Note over A,DB: Single Postgres Transaction
    A->>DB: INSERT INTO messages ON CONFLICT (app_id, idempotency_key)
    alt Duplicate key found
        DB-->>A: Existing message returned
        A-->>P: 200 OK (duplicate: true)
    else New message inserted
        A->>DB: INSERT INTO deliveries SELECT active endpoints matching event_type
        DB-->>A: Created N deliveries in state 'pending'
        A->>DB: COMMIT transaction
        A-->>P: 202 Accepted (message_id, deliveries count)
    end
```

Key guarantees in `Store.CreateMessage`:
- **Atomic fanout**: Message insertion and delivery generation occur inside single transaction. Crash cannot produce orphaned message without delivery records.
- **Idempotency**: Scoped to `(app_id, idempotency_key)`. Re-submitting returns previous message without re-enqueueing. Reusing a key with a different event type or payload returns `422`.

### Fair Queue Engine & Worker Claim

Dispatchers claim work straight from Postgres, no message broker. New messages and replays wake them through `LISTEN/NOTIFY`; a 250 ms poll catches retries coming due. Dispatcher handles rate limits, concurrency limits, and circuit breakers directly in SQL claim query.

```mermaid
flowchart TD
    Start["Dispatcher Worker Loop"] --> CheckSem{"Slots free in semaphore?<br/>Wait until >= Concurrency / 4 free"}
    CheckSem -->|Yes| ClaimProbes["Store.ClaimProbes()<br/>Find circuit breaker probe candidates"]
    ClaimProbes --> ClaimDeliveries["Store.Claim()<br/>Fair Round-Robin SQL Claim"]
    subgraph SQLClaim ["Fairness Claim Query (inside Postgres transaction)"]
        F1["Select active endpoints with due work & closed circuit"]
        F2["Filter: in_flight < max_in_flight AND GCRA tokens available"]
        F3["Order by last_claimed_at NULLS FIRST (Round-robin)"]
        F4["Lock endpoints with FOR NO KEY UPDATE SKIP LOCKED"]
        F5["Share worker slots evenly across picked endpoints"]
        F6["Update deliveries state = 'in_flight', set locked_until lease"]
        F7["Update endpoint rate_tat and last_claimed_at"]
    end
    ClaimDeliveries --> SQLClaim
    SQLClaim --> DispatchAsync["Spawn goroutine per Job"]
```

Implementation details in `Store.Claim`:
- **Noisy neighbour isolation**: Slow endpoint cannot monopolize dispatcher. `max_in_flight` (default 4) caps concurrent dispatches per endpoint.
- **GCRA rate limiting**: Generic Cell Rate Algorithm tracks theoretical arrival time (`rate_tat`) inside endpoint row. Limits hold across all dispatcher replicas without Redis.
- **`FOR NO KEY UPDATE`**: Prevents foreign key lock contention between worker claim queries and API message ingestion.
- **Lease and fencing token**: Claim sets `locked_until` (default 30 seconds). `locked_until` acts as fencing token during completion update.
- **Sweeper loop**: `Dispatcher.releaseExpiredLoop` regularly resets abandoned `in_flight` rows past lease back to `pending`.

### HTTP Dispatch & Delivery Lifecycle

```mermaid
sequenceDiagram
    autonumber
    participant D as Dispatcher Worker
    participant S as SSRF Guard
    participant R as Receiver HTTP Server
    participant DB as Postgres
    D->>D: Construct envelope: {"type", "timestamp", "data"}
    D->>D: Calculate HMAC-SHA256 signature (Standard Webhooks)
    Note over D: Dual-sign if secret rotation active
    D->>S: Dial endpoint URL
    S->>S: Resolve DNS & check IP in net.Dialer.Control
    alt Non-public / Loopback / Private IP
        S-->>D: Refuse connection (ErrBlocked)
    else Public IP
        S->>R: POST request with headers:<br/>webhook-id, webhook-timestamp, webhook-signature
        R-->>D: HTTP status + response body
    end
    D->>DB: Store.Complete() transaction
    Note over DB: 1. INSERT INTO attempts<br/>2. UPDATE deliveries state (WHERE locked_until = lease)<br/>3. UPDATE endpoint health & circuit breaker
```

#### Request Signing ([`internal/signing/signing.go`](internal/signing/signing.go))
Follows [Standard Webhooks spec](https://www.standardwebhooks.com/).

Headers sent:
- `webhook-id`: Message ID (`msg_...`).
- `webhook-timestamp`: Unix timestamp string.
- `webhook-signature`: `v1,<base64 HMAC-SHA256 of id.timestamp.body>`.

**Zero-downtime secret rotation**: Old secret stays active for configured window (default 24 hours). Dispatcher signs with both secrets; headers hold space-separated signatures ([`internal/dispatcher/dispatcher.go#L422-L430`](internal/dispatcher/dispatcher.go#L422-L430)).

#### SSRF Protection ([`internal/ssrf/ssrf.go`](internal/ssrf/ssrf.go))
- Injected via custom `net.Dialer.Control`.
- Validates resolved IP right before socket connection.
- Rejects loopback, link-local metadata (`169.254.169.254`), private RFC1918, carrier NAT, and broadcast.
- Defeats DNS rebinding attacks. HTTP redirects intentionally not followed.

### Retries, Circuit Breaker & Health

```mermaid
stateDiagram-v2
    [*] --> Pending : Fanout
    Pending --> InFlight : Claimed with 30s lease
    InFlight --> Pending : Lease expired (worker crash)
    InFlight --> Succeeded : 2xx Response
    InFlight --> Pending : Failed (Retry scheduled with jitter)
    InFlight --> Dead : 10 attempts exhausted

    state "Endpoint Circuit Breaker" as CB {
        Closed --> Open : 10 consecutive failures
        Open --> Probing : Cooldown expired (1m..1h)
        Probing --> Closed : Probe delivery 2xx
        Probing --> Open : Probe failed (backoff doubled)
    }
    Open --> Disabled : Failing continuously for 5 days
```

1. **Retry Schedule ([`internal/dispatcher/schedule.go`](internal/dispatcher/schedule.go))**: 10 attempts over 23 hours: 5s, 30s, 2m, 10m, 30m, 1h, 3h, 6h, 12h. Jitter: ±25% around base duration. Both configurable (`RETRY_SCHEDULE`, `RETRY_JITTER`). Respects `Retry-After` header on HTTP 429 and 503 responses, capped at 24 hours.
2. **Circuit Breaker ([`internal/postgres/queue.go#L325-L357`](internal/postgres/queue.go#L325-L357))**: Trips after 10 consecutive delivery failures. Normal claims pause for that endpoint. Initial cool-down: 1 minute. Once cool-down expires, `Store.ClaimProbes` claims single probe delivery. Success closes circuit and resets failure counter. Failure doubles cool-down (capped at 1 hour).
3. **Auto-Disable**: 5 days continuous failures automatically changes endpoint status to `disabled`. Atomically dispatches `endpoint.disabled` system event to the app's endpoints subscribed to it (subscribe one, or alert on `webhookd_endpoints_disabled_total`). `POST /v1/apps/{app}/endpoints/{ep}/enable` restores endpoint to active and resets counters.
4. **Replay**: Endpoints and deliveries support manual replay via API (`POST .../replay`). Delivery reset to pending with fresh retry schedule, preserving original `webhook-id` so receivers avoid duplicate actions.

### Day-Partitioned Attempt Storage

Attempt table records every HTTP interaction and error response body. It grows rapidly.
- Table partitioned by UTC day: `attempts_YYYYMMDD` ([`internal/postgres/migrations/0004_partition_attempts.sql`](internal/postgres/migrations/0004_partition_attempts.sql)).
- Hourly maintenance task (`Store.MaintainAttemptPartitions`) pre-creates upcoming 7 days of partitions.
- Partitions older than retention days (configured via `ATTEMPT_RETENTION_DAYS`) dropped instantly via `DROP TABLE`.
- Avoids bloat and heavy table-locking `DELETE` queries.

Messages and their deliveries are kept for `MESSAGE_RETENTION_DAYS` (30) once every delivery has finished, then deleted in small batches by the same hourly task. Unfinished messages are never deleted.

### SDK & Developer Tools

- [`webhooks`](webhooks): Go SDK containing API client and `VerifyRequest` helper for webhook receivers.
- [`cmd/webhookctl`](cmd/webhookctl): CLI tool for managing apps, endpoints, secret rotation, sending events, viewing attempts, triggering replays.
- [`cmd/mockreceiver`](cmd/mockreceiver): Test HTTP server verifying HMAC signatures, simulating configurable failure rates and latency.
- [`cmd/loadgen`](cmd/loadgen): Benchmarking tool measuring throughput and delivery lag under load.

## Guarantees

- **At-least-once, unordered.** Every request carries a stable `webhook-id`
  (the message id); receivers drop ids they have already processed.
- **No lost messages across crashes.** A message and its deliveries are
  written in one transaction. Dispatchers claim deliveries with
  `FOR UPDATE SKIP LOCKED` and a 30 s lease; if one dies mid-send, the lease
  expires and another dispatcher sends again. `TestCrashLosesNothing` checks
  this by `kill -9`-ing the dispatcher repeatedly (100 000 messages, 8 kills:
  none lost, 38 duplicates).

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
| [docs/architecture](docs/architecture/README.md) | An interactive architecture diagram, linked to the source |
| [docs/receiving.md](docs/receiving.md) | Receivers: the request format, verifying signatures, answering, retries |
| [docs/api.md](docs/api.md) and [docs/openapi.json](docs/openapi.json) | The HTTP API: routes, idempotency, pagination, errors |
| [docs/sdk-and-cli.md](docs/sdk-and-cli.md) | The Go SDK and `webhookctl` |
| [docs/delivery.md](docs/delivery.md) | How delivery works: fairness, retries, circuit breaker, replay, rotation |
| [docs/operations.md](docs/operations.md) | Running it: every setting, a production checklist, scaling, retention, metrics |
| [docs/writeup.md](docs/writeup.md) | The design, and why: queue, fairness, failure handling, measurements |

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
| 1 | 560 deliveries/s | 9 ms / 45 ms |
| 2 | 1 056 deliveries/s | 8 ms / 21 ms |
| 4 | 1 584 deliveries/s | 8 ms / 24 ms |

New messages wake dispatchers through `LISTEN/NOTIFY`. Before it, with
a 250 ms poll, the same runs had a steady p99 of 155 to 256 ms.

## Develop

Needs Go 1.25+. Postgres tests run when `DATABASE_URL` is set and skip
otherwise.

```sh
make up      # start the stack (Postgres on localhost:5432)
make test    # go test -race ./...  (includes the crash test at 2 000 messages)
make lint    # gofmt, go vet, staticcheck
make vuln    # govulncheck
make down

# The full crash test from the design doc:
CRASH_TEST_MESSAGES=100000 CRASH_TEST_KILLS=8 go test -run Crash -timeout 10m ./cmd/webhookd

# The replica benchmark (needs psql and a Postgres it can create databases on):
ADMIN_URL=postgres://postgres@localhost:5432/postgres scripts/bench.sh
```

## Repository Code Map

| Directory / File | Responsibility |
| --- | --- |
| [`cmd/webhookd/main.go`](cmd/webhookd/main.go) | Service entry point; boots API, dispatcher, or both. |
| [`internal/api/`](internal/api/) | HTTP route routing, auth check, input validation, ops handlers. |
| [`internal/dispatcher/`](internal/dispatcher/) | Dispatcher loop, concurrency semaphore, HTTP client dispatch, partition maintenance. |
| [`internal/postgres/`](internal/postgres/) | Database connection, migrations, store methods, queue claim queries, health updates. |
| [`internal/signing/`](internal/signing/) | Standard Webhooks HMAC signing and header verification. |
| [`internal/ssrf/`](internal/ssrf/) | `net.Dialer.Control` security filter blocking non-public network addresses. |
| [`webhooks/`](webhooks/) | Go client library and receiver verification utilities. |
| [`docs/openapi.json`](docs/openapi.json) | OpenAPI 3.1 specification validated by contract tests. |

## Roadmap

| Phase | Ships |
| ----- | ----- |
| 0 · Foundations ✓ | Layout, CI, compose, migrations, mock receiver |
| 1 · Core delivery ✓ | Apps, endpoints, send with idempotency, fan-out, dispatcher with leases, retries, signing, attempt log, SSRF guard, metrics |
| 2 · Reliability ✓ | Replay, circuit breaker and auto-disable, `Retry-After`, secret rotation, graceful drain |
| 3 · Fairness and scale ✓ | Per-endpoint concurrency and rate limits, multiple dispatchers, partitioned attempts, load generator |
| 4 · Developer experience ✓ | OpenAPI spec, Go SDK, `VerifyRequest()` for receivers, CLI, docs |
| 5 · Launch ✓ | Benchmarks in this README, write-up, v1.0 |

## Known issues

- One burst benchmark run during development showed delivery stalling after
  the burst. It has not been reproduced since. If you see deliveries sit due
  while dispatchers are idle, please open an issue with your
  `webhookd_delivery_lag_seconds` histogram and dispatcher logs.
- The claim query looks at every active endpoint, so it slows down with
  tens of thousands of endpoints (about 190 ms per claim with 10 000; see
  [docs/operations.md](docs/operations.md#scale)).

## Security

Report vulnerabilities privately, as described in [SECURITY.md](SECURITY.md).

## License

Apache-2.0
