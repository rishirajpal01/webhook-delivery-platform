# Receiving webhooks

This page is for whoever runs the endpoint: what arrives, how to check it is
genuine, and how to answer.

## What arrives

Every delivery is a `POST` with a JSON body and three headers from the
[Standard Webhooks](https://www.standardwebhooks.com/) spec:

```http
POST /hooks HTTP/1.1
Content-Type: application/json
webhook-id: msg_2c1f…
webhook-timestamp: 1791720000
webhook-signature: v1,K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4=

{"type":"order.paid","timestamp":"2026-10-12T07:00:00Z","data":{"order_id":"981","amount":4999}}
```

| Part | Meaning |
| --- | --- |
| `webhook-id` | The message id. The same on every retry and replay, so use it to drop duplicates |
| `webhook-timestamp` | Unix seconds when this attempt was sent |
| `webhook-signature` | `v1,` and the base64 HMAC-SHA256 of `id.timestamp.body` with the endpoint's secret. During a secret rotation there are two, space-separated; one match is enough |
| `type` | The event type the operator sent |
| `timestamp` | When the operator sent the event (RFC 3339, UTC). Retries keep it |
| `data` | The payload the operator sent, unchanged |

Because `type` is inside the body, the signature covers it too.

## Verify every request

In Go, the SDK does it in one call
([full example](../examples/receiver/main.go)):

```go
event, err := webhooks.VerifyRequest(secret, r) // secret: the endpoint's whsec_… secret
if err != nil {
	http.Error(w, "invalid signature", http.StatusUnauthorized)
	return
}
// event.Type, event.Timestamp, event.Data (json.RawMessage)
```

It rejects a request when no signature matches the body, or when the
timestamp is more than 5 minutes from now (so a captured request cannot be
replayed later). Keep the server's clock in sync.

In other languages, use any Standard Webhooks library (there are ones for
JavaScript, Python, Ruby, PHP, Java, Rust, C# and more), or check by hand:

1. Take the secret after `whsec_` and base64-decode it: that is the key.
2. Compute `base64(HMAC-SHA256(key, webhook-id + "." + webhook-timestamp + "." + raw body))`.
3. Accept if any `v1,<signature>` in the header equals it (compare in
   constant time), and the timestamp is within 5 minutes.

Always verify the raw bytes you received, before any JSON parsing.

## Answer

- **2xx** means delivered: the service stops retrying. Answer once the event
  is safely stored, and do slow work afterwards; requests time out after
  15 s.
- **Anything else** (or a timeout or connection error) is retried after 5 s,
  30 s, 2 m, 10 m, 30 m, 1 h, 3 h, 6 h and 12 h (±25%), about 23 hours in all.
  Redirects are not followed.
- **429 or 503 with `Retry-After`** pushes the next attempt back at least
  that long.

Delivery is at least once and unordered: the same `webhook-id` can arrive
twice, and events can arrive out of order. Record the ids you have handled,
ideally in the same database transaction as the work, and use `timestamp`
or your own data to order events when it matters.

## Service events

When an endpoint has failed continuously for 5 days the service disables it
and sends an `endpoint.disabled` event to the app's endpoints that list
`endpoint.disabled` in their `event_types`:

```json
{"type":"endpoint.disabled","timestamp":"…","data":{"endpoint_id":"ep_…","url":"https://…","failing_since":"…","disabled_at":"…"}}
```

An endpoint with an empty filter does not receive it, because an empty
filter means every event the operator sends, not the service's own.
