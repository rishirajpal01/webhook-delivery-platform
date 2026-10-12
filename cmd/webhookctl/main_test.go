package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/api"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/dispatcher"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/testdb"
	"github.com/rishirajpal01/webhook-delivery-platform/webhooks"
)

const key = "cli-test-key-0123456789"

// TestWalkthrough drives the CLI the way the README does: create an app and
// an endpoint, send, wait for the verified delivery, then a few more
// commands.
func TestWalkthrough(t *testing.T) {
	db, _ := testdb.New(t)
	ctx := context.Background()
	if _, err := postgres.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	store := postgres.NewStore(db)
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	apiSrv := httptest.NewServer(api.NewHandler(db, store, sha256.Sum256([]byte(key)), discard))
	defer apiSrv.Close()

	const secret = "whsec_Y2xpLXRlc3Qtc2VjcmV0LWNsaS10ZXN0"
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := webhooks.VerifyRequest(secret, r); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	defer receiver.Close()

	dctx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		dispatcher.New(store, dispatcher.Config{AllowPrivateNetworks: true, PollInterval: 10 * time.Millisecond}, discard).Run(dctx)
		close(done)
	}()
	defer func() { stop(); <-done }()

	env := func(k string) string {
		return map[string]string{"WEBHOOKS_API": apiSrv.URL, "API_KEY": key}[k]
	}
	ctl := func(wantCode int, args ...string) string {
		t.Helper()
		var out, errOut bytes.Buffer
		if code := run(ctx, args, env, &out, &errOut); code != wantCode {
			t.Fatalf("webhookctl %s: exit %d, want %d; stderr %s", strings.Join(args, " "), code, wantCode, errOut.String())
		}
		return strings.TrimSpace(out.String())
	}

	app := ctl(0, "-o", "{{.id}}", "apps", "create", "demo")
	ep := ctl(0, "-o", "{{.id}}", "endpoints", "create", app, receiver.URL, "-events", "order.paid", "-secret", secret)
	msg := ctl(0, "-o", "{{.id}}", "send", app, "order.paid", `{"order_id":"981"}`, "-idempotency-key", "o-981")
	if again := ctl(0, "-o", "{{.id}}", "send", "-idempotency-key", "o-981", app, "order.paid", `{"order_id":"981"}`); again != msg {
		t.Fatalf("idempotent send gave %s, want %s", again, msg)
	}
	if state := ctl(0, "-o", "{{(index .data 0).state}}", "deliveries", app, msg, "-wait", "10s"); state != "succeeded" {
		t.Fatalf("delivery state %q", state)
	}
	del := ctl(0, "-o", "{{(index .data 0).id}}", "deliveries", app, msg)
	if code := ctl(0, "-o", "{{(index .data 0).status_code}}", "attempts", app, del); code != "200" {
		t.Fatalf("attempt status %q", code)
	}

	// The rest of the commands, briefly.
	ctl(0, "apps", "get", app)
	ctl(0, "endpoints", "list", app, "-limit", "1")
	if s := ctl(0, "-o", "{{.status}}", "endpoints", "update", app, ep, "-status", "paused"); s != "paused" {
		t.Fatalf("status after update %q", s)
	}
	if s := ctl(0, "-o", "{{.status}}", "endpoints", "enable", app, ep); s != "active" {
		t.Fatalf("status after enable %q", s)
	}
	ctl(0, "endpoints", "get", app, ep)
	ctl(0, "endpoints", "rotate-secret", app, ep)
	ctl(0, "-o", "{{.replayed}}", "endpoints", "replay", app, ep, "-since", "1h")
	ctl(0, "replay", app, del)
	ctl(0, "endpoints", "delete", app, ep)

	// Errors: the API's problem, and usage mistakes.
	ctl(1, "apps", "get", "app_missing")
	ctl(2, "apps", "create")
	ctl(2, "nonsense")
	ctl(1, "send", app, "order.paid", "{not json")
}
