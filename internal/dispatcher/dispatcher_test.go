package dispatcher

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/signing"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/testdb"
)

type fixture struct {
	db     *sql.DB
	store  *postgres.Store
	app    postgres.App
	secret string
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	db, _ := testdb.New(t)
	if _, err := postgres.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	s := postgres.NewStore(db)
	app, err := s.CreateApp(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	return fixture{db: db, store: s, app: app, secret: signing.NewSecret()}
}

// send registers url as the app's only endpoint and sends one message to it.
func (f fixture) send(t *testing.T, url string) postgres.SendResult {
	t.Helper()
	ctx := context.Background()
	if _, err := f.store.CreateEndpoint(ctx, postgres.Endpoint{AppID: f.app.ID, URL: url, Secret: f.secret}); err != nil {
		t.Fatal(err)
	}
	r, err := f.store.CreateMessage(ctx, postgres.Message{
		AppID: f.app.ID, EventType: "order.paid", Payload: json.RawMessage(`{"order":"981"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// run starts a dispatcher and stops it when the test ends.
func (f fixture) run(t *testing.T, cfg Config) {
	t.Helper()
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Millisecond
	}
	d := New(f.store, cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(stopped)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
	})
}

// waitForState polls until the message's single delivery reaches state.
func (f fixture) waitForState(t *testing.T, msgID, state string) postgres.Delivery {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ds, err := f.store.ListDeliveries(context.Background(), f.app.ID, msgID, "", 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(ds) == 1 && ds[0].State == state {
			return ds[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("delivery of %s never reached %s", msgID, state)
	return postgres.Delivery{}
}

func (f fixture) attempts(t *testing.T, deliveryID string) []postgres.Attempt {
	t.Helper()
	as, err := f.store.ListAttempts(context.Background(), f.app.ID, deliveryID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	return as
}

func TestDeliversSignedRequest(t *testing.T) {
	f := newFixture(t)
	var verifyErr atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		err := signing.Verify(f.secret, r.Header.Get(signing.HeaderID), r.Header.Get(signing.HeaderTimestamp),
			r.Header.Get(signing.HeaderSignature), body, time.Now(), time.Minute)
		if err != nil {
			verifyErr.Store(err)
		}
		if r.Header.Get("Content-Type") != "application/json" ||
			!strings.HasPrefix(string(body), `{"type":"order.paid","timestamp":"`) ||
			!strings.HasSuffix(string(body), `"data":{"order":"981"}}`) {
			verifyErr.Store("unexpected request")
		}
		io.WriteString(w, "thanks")
	}))
	defer srv.Close()

	r := f.send(t, srv.URL)
	f.run(t, Config{AllowPrivateNetworks: true})
	d := f.waitForState(t, r.Message.ID, postgres.DeliverySucceeded)

	if err := verifyErr.Load(); err != nil {
		t.Fatalf("receiver rejected the request: %v", err)
	}
	as := f.attempts(t, d.ID)
	if len(as) != 1 || as[0].StatusCode == nil || *as[0].StatusCode != 200 || as[0].ResponseBody != "thanks" {
		t.Fatalf("attempts = %+v", as)
	}
}

func TestFailureIsRetriedLater(t *testing.T) {
	f := newFixture(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, strings.Repeat("x", maxResponseBody+100))
	}))
	defer srv.Close()

	r := f.send(t, srv.URL)
	f.run(t, Config{AllowPrivateNetworks: true, Schedule: []time.Duration{time.Hour}})

	deadline := time.Now().Add(10 * time.Second)
	var d postgres.Delivery
	for time.Now().Before(deadline) {
		ds, _ := f.store.ListDeliveries(context.Background(), f.app.ID, r.Message.ID, "", 100)
		if d = ds[0]; d.AttemptCount == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if d.State != postgres.DeliveryPending || d.AttemptCount != 1 {
		t.Fatalf("delivery = %+v, want pending after 1 attempt", d)
	}
	if wait := time.Until(d.NextAttemptAt); wait < 40*time.Minute {
		t.Fatalf("next attempt in %v, want about an hour", wait)
	}
	as := f.attempts(t, d.ID)
	if len(as) != 1 || *as[0].StatusCode != 500 || len(as[0].ResponseBody) != maxResponseBody {
		t.Fatalf("attempt = %+v (body %d bytes)", as, len(as[0].ResponseBody))
	}
}

func TestDeadAfterScheduleIsUsedUp(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		// Redirects are failures and are not followed.
		http.Redirect(w, r, "http://example.com/", http.StatusFound)
	}))
	defer srv.Close()

	r := f.send(t, srv.URL)
	f.run(t, Config{AllowPrivateNetworks: true, Schedule: []time.Duration{10 * time.Millisecond, 10 * time.Millisecond}})
	d := f.waitForState(t, r.Message.ID, postgres.DeliveryDead)

	if d.AttemptCount != 3 || calls.Load() != 3 {
		t.Fatalf("attempts = %d, receiver calls = %d, want 3 each", d.AttemptCount, calls.Load())
	}
}

func TestPrivateAddressesAreBlocked(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer srv.Close()

	r := f.send(t, srv.URL) // 127.0.0.1
	f.run(t, Config{Schedule: []time.Duration{}})
	d := f.waitForState(t, r.Message.ID, postgres.DeliveryDead)

	as := f.attempts(t, d.ID)
	if calls.Load() != 0 || len(as) != 1 || as[0].StatusCode != nil || !strings.Contains(as[0].Error, "not allowed") {
		t.Fatalf("calls = %d, attempts = %+v", calls.Load(), as)
	}
}
