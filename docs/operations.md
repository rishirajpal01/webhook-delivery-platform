# Operating it

## Configuration

Everything is set through environment variables. Only `DATABASE_URL` and,
for the api role, the API key are required; the rest have defaults that
suit a small deployment.

**Process**

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | none | Postgres connection string (required) |
| `DB_MAX_CONNS` | `40` | Postgres connections this process may open. Keep the sum over every process below the server's `max_connections` (100 by default), and above `DISPATCHER_CONCURRENCY` |
| `ROLE` | `all` | `api`, `dispatcher`, or `all` for both in one process |
| `LISTEN_ADDR` | `:8080` | HTTP address; dispatchers serve only health and metrics |
| `SHUTDOWN_TIMEOUT` | `10s` | Time in-flight HTTP requests get on shutdown |

**API**

| Variable | Default | Meaning |
| --- | --- | --- |
| `API_KEY` | none | Bearer token for `/v1`, at least 16 characters. The api role needs this or `API_KEY_SHA256` |
| `API_KEY_SHA256` | none | The key's SHA-256 in hex (`printf %s "$KEY" \| sha256sum`), so the key itself is never stored in config. Only the hash is kept in memory either way |
| `SECRET_ROTATION_OVERLAP` | `24h` | How long a rotated-out signing secret keeps signing requests alongside the new one. `0` switches at once |

**Dispatcher**

| Variable | Default | Meaning |
| --- | --- | --- |
| `DISPATCHER_CONCURRENCY` | `16` | Deliveries one process sends at once |
| `DISPATCHER_REQUEST_TIMEOUT` | `15s` | Limit on one HTTP request to a receiver |
| `DISPATCHER_LEASE` | `30s` | How long a claimed delivery stays reserved. Must be longer than `DISPATCHER_REQUEST_TIMEOUT`, or slow sends would be sent twice; webhookd refuses to start otherwise |
| `DISPATCHER_POLL_INTERVAL` | `250ms` | How often an idle dispatcher checks for due work (retries coming due, missed notifications) |
| `DISPATCHER_LISTEN` | `true` | `LISTEN` for new messages and replays so they are sent at once rather than on the next poll. Set `false` behind a pooler in transaction mode (PgBouncer), which does not support `LISTEN` |
| `RETRY_SCHEDULE` | `5s,30s,2m,10m,30m,1h,3h,6h,12h` | Wait before each retry, comma-separated. One more attempt than entries: the default is ten attempts over about 23 hours |
| `RETRY_JITTER` | `0.25` | Each wait moves by up to this fraction either way, so retries after an outage spread out. `0` turns it off |
| `CIRCUIT_FAILURE_THRESHOLD` | `10` | Failures in a row that open an endpoint's circuit breaker |
| `CIRCUIT_COOLDOWN` | `1m` | Wait before the first probe of an open circuit; it doubles after each failed probe |
| `CIRCUIT_COOLDOWN_MAX` | `1h` | Longest wait between probes |
| `ENDPOINT_DISABLE_AFTER` | `120h` | How long an endpoint may fail continuously before it is disabled |
| `ALLOW_PRIVATE_NETWORKS` | `false` | Turns the SSRF guard off. Local development only |

**Retention**

| Variable | Default | Meaning |
| --- | --- | --- |
| `ATTEMPT_RETENTION_DAYS` | `30` | Days of the attempt log to keep; older days are dropped whole. `0` keeps everything |
| `MESSAGE_RETENTION_DAYS` | `30` | Days to keep a message once all its deliveries have finished (`succeeded` or `dead`); its deliveries and attempts go with it. Unfinished messages are never deleted. `0` keeps everything |

Per-endpoint limits (`max_in_flight`, `rate_limit`) are set on each endpoint
through the API, not here.

Any number of `api` and `dispatcher` processes can run against one Postgres.
With compose: `docker compose --profile scale up --build --scale dispatcher=3`.

`webhookd healthcheck` exits 0 when the webhookd on `LISTEN_ADDR` answers
`/readyz`, for container health checks (the image has no shell or curl).
Readiness means Postgres is reachable and migrations have run.

The attempt log is partitioned by day (UTC). Dispatchers create partitions
for the next 7 days at start and hourly, drop the ones past retention, and
delete finished messages past retention in batches of 1000.

## Running in production

`compose.yaml` is for trying it out on your own machine: it turns the SSRF
guard off, uses a known API key and database password, and binds every port
to localhost. For a real deployment:

- **Put TLS in front.** webhookd serves plain HTTP. Run it behind a reverse
  proxy or load balancer that terminates TLS (Caddy, nginx, a cloud load
  balancer).
- **Keep `/metrics` private.** `/healthz`, `/readyz`, `/metrics` and
  `/openapi.json` need no key and share the API's port. Expose only `/v1`
  through the proxy, or firewall the rest.
- **The API key is an admin key.** One key reaches every app. It is meant for
  your own backend, never for your customers. Generate a long random one
  (`openssl rand -hex 32`) and configure it as `API_KEY_SHA256`.
- **Leave the SSRF guard on.** Never set `ALLOW_PRIVATE_NETWORKS` in
  production; your customers choose the URLs the service calls.
- **Size connections.** Each process opens up to `DB_MAX_CONNS` connections,
  plus one held for `LISTEN` on dispatchers.
- **Give shutdown time to drain.** On SIGTERM a dispatcher finishes the sends
  it started (up to `DISPATCHER_REQUEST_TIMEOUT`) and records them (up to
  10 s more). Allow at least 30 s before a hard kill:
  `stop_grace_period: 30s` in compose, `terminationGracePeriodSeconds: 30` in
  Kubernetes. A hard kill loses nothing, but the interrupted sends are sent
  again after their lease expires.
- **Protect the database and its backups.** Endpoint signing secrets are
  stored in Postgres as they are, so anyone who can read the database can
  sign requests as you.
- **Subscribe to `endpoint.disabled`.** When an endpoint is disabled the
  service sends an `endpoint.disabled` event only to endpoints that list it
  in `event_types`. If none does, alert on `webhookd_endpoints_disabled_total`
  instead, or nobody will notice.
- **Postgres version.** Tested on Postgres 16.

## Endpoint status and what happens to events

| Status | New events | Queued deliveries |
| --- | --- | --- |
| `active` | Delivered | Delivered |
| `paused` (set by you) | Queued, and sent after you resume it | Wait until you resume it |
| `disabled` (set by the service after `ENDPOINT_DISABLE_AFTER` of failures) | Not queued for this endpoint | Wait until you re-enable it |

A paused endpoint keeps collecting events for as long as it is paused, and
messages are not deleted while a delivery is still waiting. Pause for hours or
days, not indefinitely.

## Scale

Each claim looks at every active endpoint, so its cost grows with the number
of endpoints. Measured on a 4-CPU VM with Postgres on the same machine, one
claim takes about 4 ms with 20 endpoints and a 60 000-delivery backlog, about
50 ms with 10 000 endpoints that all have due work, and about 190 ms with
10 000 endpoints of which only 100 have due work. Up to a few thousand
endpoints this is fine; at tens of thousands, measure before relying on it.

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
