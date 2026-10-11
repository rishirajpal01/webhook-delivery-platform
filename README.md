# webhook-delivery-platform

A self-hostable webhook delivery service: your product sends an event once,
and the service delivers it to every subscribed endpoint, retrying with
backoff, signing each request with [Standard Webhooks](https://www.standardwebhooks.com/),
keeping one slow endpoint from delaying the rest, and recording every attempt.

One Go binary plus Postgres. No Kafka, Redis or Kubernetes required.

> **Status: phase 0 (foundations).** The service starts, applies its schema and
> serves health checks. Delivery lands in phase 1.

## Guarantees

- **At-least-once, unordered.** Every request carries a stable `webhook-id`;
  receivers drop duplicates they have already seen.
- Postgres is the queue: deliveries are claimed with `FOR UPDATE SKIP LOCKED`
  plus a lease, so a crashed worker's rows are picked up again.

## Run it

```sh
docker compose up --build
curl localhost:8080/readyz     # {"status":"ok"} once Postgres is reachable
```

This starts Postgres, `webhookd` on `:8080` and a mock receiver on `:9000`
that fails 30% of requests, so retries are easy to watch.

## Develop

Needs Go 1.25+. Postgres tests run when `DATABASE_URL` is set and skip
otherwise.

```sh
make up      # start the stack (Postgres on localhost:5432)
make test    # go test -race ./...
make lint    # gofmt, go vet, staticcheck
make down
```

## Configuration

| Variable           | Default | Meaning                                   |
| ------------------ | ------- | ----------------------------------------- |
| `DATABASE_URL`     | none    | Postgres connection string (required)     |
| `LISTEN_ADDR`      | `:8080` | HTTP listen address                       |
| `SHUTDOWN_TIMEOUT` | `10s`   | Time in-flight requests get on shutdown   |

## Layout

```
cmd/webhookd       the service
cmd/mockreceiver   a receiver for demos and tests (-fail-rate, -latency)
internal/api       HTTP routes
internal/config    environment config
internal/postgres  connection and embedded migrations
```

## Roadmap

| Phase | Ships |
| ----- | ----- |
| 0 · Foundations | Layout, CI, compose, migrations, mock receiver |
| 1 · Core delivery | Apps, endpoints, send with idempotency, fan-out, dispatcher with leases, retries, signing, attempt log, SSRF guard, metrics |
| 2 · Reliability | Dead state, replay, circuit breaker, `Retry-After`, secret rotation, graceful drain |
| 3 · Fairness and scale | Per-endpoint concurrency and rate limits, multiple dispatchers, partitioned attempts, load generator |
| 4 · Developer experience | OpenAPI spec, Go SDK, `Verify()` helper, CLI |
| 5 · Launch | Benchmarks in this README, write-up, v1.0 |

## License

Apache-2.0
