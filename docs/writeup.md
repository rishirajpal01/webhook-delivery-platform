# Building a webhook delivery service on one Postgres

*How webhook-delivery-platform delivers events at least once, keeps one bad
endpoint from slowing everyone else down, and what it measured.*

Every SaaS product ends up sending webhooks, and most have an incident story
about them: retries hammering a dead endpoint, one big customer's backlog
delaying everyone else, a receiver unable to tell a real request from a
forged one, or nobody able to answer "did you send it?". Hosted products
such as Svix and Hookdeck exist because getting this right is harder than
it looks. This project is a small, self-hostable version built to the same
standards: one Go binary and one Postgres.

## The promise

The service promises **at-least-once, unordered** delivery, and says so
loudly. Exactly-once does not exist over HTTP: a receiver can process a
request and crash before answering, and the sender cannot tell that apart
from a request that never arrived. So every request carries a stable
`webhook-id`, and receivers drop ids they have seen.

Two numbers make the promise concrete:

- **Zero lost messages across `kill -9`.** A crash test sends 100 000 messages and
  `kill -9`s the dispatcher 8 times: none lost, 38 delivered twice.
- **A healthy endpoint's p99 lag stays under 1 s while a noisy neighbour is
  backlogged.** It measures about 30 ms.

## One binary, two roles, one database

`webhookd` runs as `api`, `dispatcher` or both. The API writes; dispatchers
send. They never talk to each other: Postgres is the only shared state, and
any number of either can run side by side.

A send is one transaction: insert the message, then one `deliveries` row per
active endpoint subscribed to its event type. A crash can never leave a
message with no deliveries, and there is no outbox or second system to keep
in step. The delivery row *is* the queue item.

## A queue in a table

A dispatcher claims due rows with `SELECT … FOR UPDATE SKIP LOCKED`, marks
them `in_flight` with a lease (`locked_until`, 30 s), commits, and only then
makes the HTTP call. Holding a transaction open across a network call would
pin a connection and a lock for up to 15 s; the lease makes that
unnecessary. If the dispatcher dies mid-send, the lease expires, a sweeper
returns the row to `pending`, and another dispatcher sends it again. That is
exactly where "at least once" comes from.

The lease value doubles as a **fencing token**. When the attempt is
recorded, the state update only applies `WHERE locked_until = <the lease I
was given>`. A dispatcher that stalled past its lease (a long GC pause, a
frozen VM) cannot overwrite the outcome of the dispatcher that took the row
over. Its attempt is still logged, because the HTTP call really happened.

## Fairness: the claim query is the scheduler

The classic webhook outage is head-of-line blocking. Claim the oldest due
rows first, and one endpoint with a 50 000-message backlog that answers in
10 s occupies every worker, so everyone else waits.

Each endpoint has a concurrency cap (`max_in_flight`, default 4) and an
optional rate limit. Both are enforced **inside the claim**, in one
transaction:

1. Pick endpoints that are active, have a closed circuit, are under their
   cap, have a rate-limit token and have due work. Order them by when they
   were last served (round robin), and lock them with `SKIP LOCKED` so two
   dispatchers never hand out the same endpoint's slots.
2. Share the free worker slots out between them.
3. Claim that many due deliveries per endpoint.

The rate limit is GCRA, a token bucket stored as one timestamp per endpoint
(the "theoretical arrival time"). Because the limiter state lives in the row
the claim already locks, it is shared by every replica for free: no Redis,
no coordination. Two details mattered in practice:

- The locks are `FOR NO KEY UPDATE`, not `FOR UPDATE`. A plain `FOR UPDATE`
  on an endpoint row blocks the foreign-key check of every send inserting a
  delivery for that endpoint.
- A dispatcher claims in batches (it waits until a quarter of its slots are
  free) instead of one row at a time. That leaves some slots idle, but
  together with the lock change it raised single-replica throughput from 458
  to 576 deliveries a second.

The test that matters is the one from the design doc. One endpoint answers
in 10 s (or returns 500) with 500 deliveries queued, and nine healthy
endpoints receive 90 deliveries a second. The healthy p99 lag is about 30 ms.
With the cap lifted, the slow endpoint's backlog takes every slot and the
test fails.

## Failing well

- **Retries** follow 5 s, 30 s, 2 m, 10 m, 30 m, 1 h, 3 h, 6 h, 12 h, then
  `dead`: ten attempts over about 23 hours. The jitter is ±25%, not "full
  jitter". Full jitter picks a delay anywhere from zero to the full value, so
  the nine delays would average about 11 hours in total, half the window
  receivers are promised.
- **`Retry-After`** on a 429 or 503 is honoured, capped at 24 h.
- **Circuit breaker.** After 10 straight failures an endpoint stops being
  sent to. After a cool-down (1 minute, doubling to 1 hour) one probe goes
  out; success reopens the flow. The breaker is just two columns read by
  the claim query.
- **Auto-disable** after 5 days of continuous failure, with an
  `endpoint.disabled` event sent in the same transaction, so it goes out
  exactly once.
- **Replay** resets a delivery to `pending` with the same `webhook-id`, so
  receivers that already processed it still drop it.

## Receivers can verify everything

Requests use [Standard Webhooks](https://www.standardwebhooks.com/): an HMAC
over `id.timestamp.body`, and the recommended body
`{"type", "timestamp", "data"}`, so the event type is covered by the
signature. During a secret rotation each request carries two signatures for
24 hours, so receivers switch over when they like. Any Standard Webhooks
library verifies them; the Go SDK's `VerifyRequest` does it in one call.

The sender is protected too. Webhook senders are a classic SSRF hole: a
customer registers `http://169.254.169.254/` and reads your cloud metadata.
The dialer refuses loopback, private, link-local and reserved addresses
*after* DNS resolution, which also defeats DNS rebinding.

## Keeping the log cheap

Every attempt is stored, so the attempt table grows fastest. It is
range-partitioned by day. Dispatchers create the next week's partitions
hourly and drop days past retention (30 by default). Dropping a partition
is instant and leaves no dead rows for vacuum, which `DELETE … WHERE
attempted_at < …` would.

## Measured

On a 4-CPU Linux VM, with Postgres, the API, the dispatchers and the load
generator all on the same machine. Each message fans out to 20 endpoints on
a receiver that answers in 20 ms. Lag is end to end, from the send call to
the request arriving.

| Dispatchers | Burst throughput (60 000 deliveries) | Steady lag at 400 deliveries/s, p50 / p99 |
| --- | --- | --- |
| 1 | 560 deliveries/s | 9 ms / 45 ms |
| 2 | 1 056 deliveries/s | 8 ms / 21 ms |
| 4 | 1 584 deliveries/s | 8 ms / 24 ms |

Every run delivered every message with no duplicates. `scripts/bench.sh`
reproduces the table.

New messages wake dispatchers through `LISTEN/NOTIFY`, at the price of one
held connection per dispatcher. Before that, an idle dispatcher polled four
times a second and the poll was most of the lag: p99 was 155 to 256 ms on the
same setup. One honest caveat: 2 to 4 replicas
gains less than 1 to 2 because the whole stack shares 4 CPUs. On separate
machines, the ceiling is Postgres's write rate: each delivery costs about
three row writes (claim, attempt, completion).

## The developer side

The API is described by an OpenAPI 3.1 file that the server also serves.
That file is the source of truth: contract tests fail CI if a route, a
request field or a response field differs between the spec, the server and
the Go SDK. The SDK is hand-written on the standard library, and a small CLI
sits on top of it. CI also runs the README's quickstart block word for word
on every push, so the README cannot go stale.

## What I would do next

- Opt-in FIFO per endpoint, for receivers that need order.
- Benchmarks with Postgres on its own machine, to find the real ceiling.

The code, design doc, tests and benchmark script are in the repository.
`docker compose up` and the README quickstart reproduce a verified delivery
in under a minute.
