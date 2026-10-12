package webhooks_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/api"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/dispatcher"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/testdb"
	"github.com/rishirajpal01/webhook-delivery-platform/webhooks"
)

const apiKey = "sdk-test-key-0123456789"

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// stack runs the real API against a throwaway database and returns an SDK
// client for it, the store for starting a dispatcher, and the API's URL.
func stack(t *testing.T) (*webhooks.Client, *postgres.Store, string) {
	t.Helper()
	db, _ := testdb.New(t)
	if _, err := postgres.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	store := postgres.NewStore(db)
	srv := httptest.NewServer(api.NewHandler(db, store, sha256.Sum256([]byte(apiKey)), discard))
	t.Cleanup(srv.Close)
	return webhooks.NewClient(srv.URL, apiKey), store, srv.URL
}

// TestEndToEnd sends an event with the SDK and receives it with
// VerifyRequest, through the real API and dispatcher.
func TestEndToEnd(t *testing.T) {
	c, store, _ := stack(t)
	ctx := context.Background()

	events := make(chan *webhooks.Event, 1)
	var secret string
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e, err := webhooks.VerifyRequest(secret, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		events <- e
	}))
	defer receiver.Close()

	app, err := c.CreateApp(ctx, webhooks.CreateAppRequest{Name: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	ep, err := c.CreateEndpoint(ctx, app.ID, webhooks.CreateEndpointRequest{URL: receiver.URL, EventTypes: []string{"order.paid"}})
	if err != nil {
		t.Fatal(err)
	}
	secret = ep.Secret

	dctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		dispatcher.New(store, dispatcher.Config{AllowPrivateNetworks: true, PollInterval: 10 * time.Millisecond}, discard).Run(dctx)
		close(done)
	}()
	defer func() { stop(); <-done }()

	sent, err := c.SendMessage(ctx, app.ID, webhooks.SendMessageRequest{
		EventType: "order.paid", Payload: map[string]any{"order_id": "981"},
	})
	if err != nil || sent.Deliveries != 1 {
		t.Fatalf("send = %+v, %v", sent, err)
	}
	select {
	case e := <-events:
		var data struct {
			OrderID string `json:"order_id"`
		}
		if err := json.Unmarshal(e.Data, &data); err != nil || e.Type != "order.paid" || data.OrderID != "981" || e.Timestamp.IsZero() {
			t.Fatalf("received %+v (%s), %v", e, e.Data, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the event never arrived")
	}

	// The attempt log shows the verified delivery.
	var ds *webhooks.Page[webhooks.Delivery]
	for range 100 {
		if ds, err = c.ListDeliveries(ctx, app.ID, sent.ID, webhooks.ListOptions{}); err == nil && ds.Data[0].State == "succeeded" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ds.Data[0].State != "succeeded" {
		t.Fatalf("delivery = %+v", ds.Data[0])
	}
	as, err := c.ListAttempts(ctx, app.ID, ds.Data[0].ID, webhooks.ListOptions{})
	if err != nil || len(as.Data) != 1 || as.Data[0].StatusCode == nil || *as.Data[0].StatusCode != 200 {
		t.Fatalf("attempts = %+v, %v", as, err)
	}
}

// TestClientMethods calls every method once against the real API.
func TestClientMethods(t *testing.T) {
	c, _, url := stack(t)
	ctx := context.Background()

	app, err := c.CreateApp(ctx, webhooks.CreateAppRequest{Name: "acme", IdempotencyKey: "app-1"})
	if err != nil {
		t.Fatal(err)
	}
	if again, err := c.CreateApp(ctx, webhooks.CreateAppRequest{Name: "acme", IdempotencyKey: "app-1"}); err != nil || again.ID != app.ID {
		t.Fatalf("idempotent create = %+v, %v", again, err)
	}
	if got, err := c.GetApp(ctx, app.ID); err != nil || got.Name != "acme" {
		t.Fatalf("GetApp = %+v, %v", got, err)
	}

	ep, err := c.CreateEndpoint(ctx, app.ID, webhooks.CreateEndpointRequest{
		URL: "https://example.com/hooks", MaxInFlight: 8, RateLimit: 20, IdempotencyKey: "ep-1",
	})
	if err != nil || ep.Secret == "" || ep.MaxInFlight != 8 || ep.RateLimit != 20 {
		t.Fatalf("CreateEndpoint = %+v, %v", ep, err)
	}
	if _, err := c.CreateEndpoint(ctx, app.ID, webhooks.CreateEndpointRequest{URL: "https://example.com/other"}); err != nil {
		t.Fatal(err)
	}
	page, err := c.ListEndpoints(ctx, app.ID, webhooks.ListOptions{Limit: 1})
	if err != nil || len(page.Data) != 1 || page.NextCursor == "" {
		t.Fatalf("ListEndpoints = %+v, %v", page, err)
	}
	if page, err = c.ListEndpoints(ctx, app.ID, webhooks.ListOptions{Limit: 1, Cursor: page.NextCursor}); err != nil || len(page.Data) != 1 {
		t.Fatalf("second page = %+v, %v", page, err)
	}
	if got, err := c.GetEndpoint(ctx, app.ID, ep.ID); err != nil || got.Secret != "" || got.URL != ep.URL {
		t.Fatalf("GetEndpoint = %+v, %v", got, err)
	}
	upd, err := c.UpdateEndpoint(ctx, app.ID, ep.ID, webhooks.UpdateEndpointRequest{
		Status: webhooks.Ptr("paused"), EventTypes: &[]string{"order.paid"}, RateLimit: webhooks.Ptr(0),
	})
	if err != nil || upd.Status != "paused" || len(upd.EventTypes) != 1 || upd.RateLimit != 0 {
		t.Fatalf("UpdateEndpoint = %+v, %v", upd, err)
	}
	if en, err := c.EnableEndpoint(ctx, app.ID, ep.ID); err != nil || en.Status != "active" {
		t.Fatalf("EnableEndpoint = %+v, %v", en, err)
	}
	if rot, err := c.RotateSecret(ctx, app.ID, ep.ID, webhooks.RotateSecretRequest{}); err != nil || rot.Secret == "" || rot.Secret == ep.Secret {
		t.Fatalf("RotateSecret = %+v, %v", rot, err)
	}

	msg := webhooks.SendMessageRequest{EventType: "order.paid", Payload: json.RawMessage(`{"n":1}`), IdempotencyKey: "m-1"}
	sent, err := c.SendMessage(ctx, app.ID, msg)
	if err != nil || sent.Replayed || sent.Deliveries != 2 {
		t.Fatalf("SendMessage = %+v, %v", sent, err)
	}
	if again, err := c.SendMessage(ctx, app.ID, msg); err != nil || !again.Replayed || again.ID != sent.ID {
		t.Fatalf("repeat SendMessage = %+v, %v", again, err)
	}
	ds, err := c.ListDeliveries(ctx, app.ID, sent.ID, webhooks.ListOptions{})
	if err != nil || len(ds.Data) != 2 {
		t.Fatalf("ListDeliveries = %+v, %v", ds, err)
	}
	if as, err := c.ListAttempts(ctx, app.ID, ds.Data[0].ID, webhooks.ListOptions{}); err != nil || len(as.Data) != 0 {
		t.Fatalf("ListAttempts = %+v, %v", as, err)
	}

	// A pending delivery cannot be replayed: the API's problem comes back
	// as *webhooks.Error.
	var apiErr *webhooks.Error
	if _, err := c.ReplayDelivery(ctx, app.ID, ds.Data[0].ID); !errors.As(err, &apiErr) || apiErr.StatusCode != 409 || apiErr.Detail == "" {
		t.Fatalf("ReplayDelivery of a pending delivery: %v", err)
	}
	if n, err := c.ReplayEndpoint(ctx, app.ID, ep.ID, time.Now().Add(-time.Hour)); err != nil || n != 0 {
		t.Fatalf("ReplayEndpoint = %d, %v", n, err)
	}

	if err := c.DeleteEndpoint(ctx, app.ID, ep.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetEndpoint(ctx, app.ID, ep.ID); !errors.As(err, &apiErr) || apiErr.StatusCode != 404 {
		t.Fatalf("GetEndpoint after delete: %v", err)
	}
	if _, err := webhooks.NewClient(url, "wrong-key").GetApp(ctx, app.ID); !errors.As(err, &apiErr) || apiErr.StatusCode != 401 {
		t.Fatalf("wrong key: %v", err)
	}
}
