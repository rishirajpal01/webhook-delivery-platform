# Go SDK and CLI

## Go SDK

```sh
go get github.com/rishirajpal01/webhook-delivery-platform/webhooks
```

Package `webhooks` uses only the standard library. A `Client` method exists
for every operation in [openapi.json](openapi.json), with the same names
(`createEndpoint` is `CreateEndpoint`); a test fails if they drift apart.

```go
c := webhooks.NewClient("http://localhost:8080", os.Getenv("API_KEY"))

app, err := c.CreateApp(ctx, webhooks.CreateAppRequest{Name: "acme", IdempotencyKey: "acme"})
ep, err := c.CreateEndpoint(ctx, app.ID, webhooks.CreateEndpointRequest{
	URL:        "https://acme.example/hooks",
	EventTypes: []string{"order.paid"},
})
// ep.Secret is returned only now: give it to the receiver.

msg, err := c.SendMessage(ctx, app.ID, webhooks.SendMessageRequest{
	EventType:      "order.paid",
	Payload:        map[string]any{"order_id": "981", "amount": 4999},
	IdempotencyKey: "order-981-paid", // a retried send returns the same message
})

page, err := c.ListDeliveries(ctx, app.ID, msg.ID, webhooks.ListOptions{})
for _, d := range page.Data {
	fmt.Println(d.EndpointID, d.State, d.AttemptCount)
}
// page.NextCursor, passed as ListOptions.Cursor, fetches the next page.

// Optional fields of an update are pointers; Ptr fills them in.
_, err = c.UpdateEndpoint(ctx, app.ID, ep.ID, webhooks.UpdateEndpointRequest{Status: webhooks.Ptr("paused")})
```

API errors are `*webhooks.Error`, carrying the RFC 9457 problem:

```go
var apiErr *webhooks.Error
if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound { … }
```

For receivers, `webhooks.VerifyRequest` checks the signature and decodes the
event: see [receiving.md](receiving.md).

## CLI

`webhookctl` is a thin layer over the SDK. It ships in the Docker image, so
with the compose stack running no local Go is needed:

```sh
ctl() { docker compose exec -T webhookd webhookctl "$@"; }
```

or build it: `go install github.com/rishirajpal01/webhook-delivery-platform/cmd/webhookctl@latest`.
It reads `WEBHOOKS_API` (default `http://localhost:8080`) and `API_KEY`, or
the `-api` and `-key` flags.

```
webhookctl [-api URL] [-key KEY] [-o TEMPLATE] COMMAND ARGS [FLAGS]

  apps create NAME [-idempotency-key K]
  apps get APP
  endpoints create APP URL [-events a,b] [-secret whsec_…] [-max-in-flight N] [-rate-limit N] [-idempotency-key K]
  endpoints list APP [-limit N] [-cursor C]
  endpoints get APP EP
  endpoints update APP EP [-url U] [-events a,b] [-status active|paused] [-max-in-flight N] [-rate-limit N]
  endpoints delete APP EP
  endpoints enable APP EP
  endpoints rotate-secret APP EP [-secret whsec_…]
  endpoints replay APP EP -since TIME   (RFC 3339, or a duration such as 2h meaning that long ago)
  send APP EVENT_TYPE PAYLOAD_JSON [-idempotency-key K]
  deliveries APP MSG [-limit N] [-cursor C] [-wait DURATION]
  attempts APP DEL [-limit N] [-cursor C]
  replay APP DEL
```

Output is the API's JSON. `-o` takes a Go template over it instead, which is
handy in scripts:

```sh
APP=$(webhookctl -o '{{.id}}' apps create acme)
webhookctl -o '{{range .data}}{{.endpoint_id}} {{.state}}{{"\n"}}{{end}}' deliveries $APP $MSG
```

`deliveries -wait 2m` polls until no delivery is pending or in flight and
exits 0 only if every one succeeded. Exit status is 1 for API errors and 2
for usage mistakes.
