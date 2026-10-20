# How delivery works

The promise is at-least-once, unordered delivery: every request carries a
stable `webhook-id`, and receivers drop ids they have already processed.

- **Fairness:** each endpoint gets at most `max_in_flight` concurrent sends
  (default 4, up to 100) and, if `rate_limit` is set, that many requests per
  second (bursts of up to one second's worth). Dispatchers serve endpoints
  round-robin and skip any endpoint at its limit, so one slow or backlogged
  endpoint cannot hold up the rest. The limits live in Postgres and are
  enforced inside the claim, so they hold across all replicas.
  `TestNoisyNeighbour` checks this the way the design doc states it: an
  endpoint that answers in 10 s, or one that returns 500, with 500
  deliveries queued, leaves nine healthy endpoints at a p99 lag of about
  30 ms. With the cap lifted the slow case fails: its backlog takes every
  slot.

- **Retries** on network errors, timeouts, redirects and any non-2xx, after
  5 s, 30 s, 2 m, 10 m, 30 m, 1 h, 3 h, 6 h and 12 h (each ±25% jitter), then
  the delivery is `dead`. Ten attempts over about 23 hours. The schedule and
  the jitter are configurable (`RETRY_SCHEDULE`, `RETRY_JITTER`).
- **Timeouts:** 15 s per request (`DISPATCHER_REQUEST_TIMEOUT`); redirects
  are not followed.
- **Pickup:** a new message or replay wakes dispatchers through Postgres
  `LISTEN/NOTIFY`, so it is sent at once; retries are found by a poll every
  250 ms.
- **SSRF guard:** connections to loopback, private, link-local (including
  cloud metadata at 169.254.169.254) and reserved addresses are refused,
  including IPv6 forms that embed one (NAT64, 6to4, Teredo, IPv4-mapped and
  IPv4-compatible). The check runs after DNS resolution, so DNS rebinding
  cannot get around it.
- **Signing:** `webhook-id`, `webhook-timestamp` and
  `webhook-signature: v1,<base64 HMAC-SHA256 of id.timestamp.body>`, so any
  Standard Webhooks library can verify requests.
- **`Retry-After`:** on a 429 or 503 the next attempt waits at least as long
  as the receiver asked (seconds or an HTTP date, capped at 24 h).
- **Circuit breaker:** after 10 failures in a row an endpoint's deliveries
  stop being sent. After 1 minute one probe delivery is tried; if it fails
  the wait doubles (up to 1 h), if it succeeds everything queued flows
  again. Other endpoints are unaffected. All three numbers are settings
  (`CIRCUIT_*` in [operations.md](operations.md)).
- **Pause:** a paused endpoint keeps queueing events; they are sent once it
  is set back to `active`.
- **Auto-disable:** an endpoint that has failed continuously for 5 days
  (`ENDPOINT_DISABLE_AFTER`) is set to `disabled` (`disabled_at` shows
  when). Its queued deliveries wait, and new events are not queued for it
  until `POST …/enable` re-enables it. The service also sends an
  `endpoint.disabled` event in the endpoint's app, with payload
  `{"endpoint_id", "url", "failing_since", "disabled_at"}`, to the app's
  endpoints that list `endpoint.disabled` in their `event_types` (an empty
  filter covers the operator's own events, not this one).
- **Replay:** sends a delivery again with the same `webhook-id`, so receivers
  that already processed it can still drop it. It starts a fresh retry
  schedule.
- **Secret rotation:** for 24 h (`SECRET_ROTATION_OVERLAP`) after a
  rotation each request carries two signatures, one per secret, so
  receivers can switch at their own pace.
- **Graceful drain:** on SIGTERM a dispatcher stops claiming, finishes the
  sends it started and records them before exiting.
