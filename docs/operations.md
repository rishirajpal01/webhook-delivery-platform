# Operating it

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `DATABASE_URL` | none | Postgres connection string (required) |
| `ROLE` | `all` | `api`, `dispatcher`, or `all` for both in one process |
| `API_KEY` | none | Bearer token for `/v1`, at least 16 characters. The api role needs this or `API_KEY_SHA256` |
| `API_KEY_SHA256` | none | The key's SHA-256 in hex (`printf %s "$KEY" \| sha256sum`), so the key itself is never stored in config. Only the hash is kept in memory either way |
| `LISTEN_ADDR` | `:8080` | HTTP address; dispatchers serve only health and metrics |
| `DISPATCHER_CONCURRENCY` | `16` | Deliveries one process sends at once |
| `DISPATCHER_LEASE` | `30s` | How long a claimed delivery stays reserved; keep it above the 15 s request timeout |
| `ALLOW_PRIVATE_NETWORKS` | `false` | Turns the SSRF guard off. Local development only |
| `SHUTDOWN_TIMEOUT` | `10s` | Time in-flight HTTP requests get on shutdown |
| `ATTEMPT_RETENTION_DAYS` | `30` | Days of the attempt log to keep; older days are dropped whole. `0` keeps everything |

Any number of `api` and `dispatcher` processes can run against one Postgres.
With compose: `docker compose --profile scale up --build --scale dispatcher=3`.

`webhookd healthcheck` exits 0 when the webhookd on `LISTEN_ADDR` answers
`/readyz`, for container health checks (the image has no shell or curl).
Readiness means Postgres is reachable and migrations have run.

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
