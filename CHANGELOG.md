# Changelog

## v1.0.0

The first release: everything in the design doc, built in five phases.

**Delivery**

- Send an event once over the API; it is stored with one delivery per
  subscribed endpoint in a single transaction, and delivered at least once.
  `kill -9` of any process loses nothing (crash test: 100 000 messages,
  8 kills).
- Requests are signed with [Standard Webhooks](https://www.standardwebhooks.com/)
  and carry its recommended body, `{"type", "timestamp", "data"}`.
- Retries after 5 s, 30 s, 2 m, 10 m, 30 m, 1 h, 3 h, 6 h and 12 h (±25%
  jitter), honouring `Retry-After`, then `dead`. Every attempt is logged with
  status, latency, error and the first 4 KB of the response.
- Per-endpoint concurrency cap (default 4) and rate limit, enforced in the
  claim query so they hold across any number of dispatcher replicas. One
  slow or failing endpoint does not delay the rest (noisy-neighbour test:
  healthy p99 lag about 30 ms against a 1 s target).
- Circuit breaker after 10 straight failures, auto-disable after 5 days with
  an `endpoint.disabled` event, replay of one delivery or everything dead
  since a time, secret rotation with 24 h of dual signing, SSRF guard,
  graceful drain.
- Attempt log partitioned by day with 30-day retention, and finished
  messages (with their deliveries) deleted after 30 days.
- New messages and replays wake dispatchers through `LISTEN/NOTIFY`; the
  250 ms poll remains for retries.
- A paused endpoint keeps queueing events and receives them when resumed.
- Every tuning knob is an environment variable: connection pool, request
  timeout, lease, poll interval, retry schedule and jitter, circuit breaker,
  auto-disable, secret rotation overlap and retention. See
  `docs/operations.md`.

**API and tools**

- REST API under `/v1` described by `docs/openapi.json` (OpenAPI 3.1), with
  `Idempotency-Key` on every create (reusing a key for a different request
  is refused with 422), cursor pagination and RFC 9457 errors.
- `GET /v1/apps/{app}/messages` lists an app's messages, newest first.
- Go SDK (`webhooks`, standard library only) with `VerifyRequest` for
  receivers, and the `webhookctl` CLI. Contract tests keep the server and
  the SDK in line with the spec.
- `docker compose up` brings up Postgres, the service and a mock receiver,
  bound to localhost; CI runs the README quickstart as written on every
  push, plus `govulncheck`.
- Each `v*` tag publishes `ghcr.io/rishirajpal01/webhook-delivery-platform`
  for amd64 and arm64.

**Upgrading from pre-release builds**

- Receivers now get the envelope `{"type", "timestamp", "data"}` instead of
  the bare payload.
- Migrations 0001 to 0007 run on start, in one process at a time.
- webhookd refuses to start if `DISPATCHER_LEASE` is not longer than the
  request timeout (15 s unless `DISPATCHER_REQUEST_TIMEOUT` is set).
