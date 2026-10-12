package dispatcher

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/signing"
)

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"120", 2 * time.Minute},
		{"-5", 0},
		{"soon", 0},
		{now.Add(90 * time.Second).Format(http.TimeFormat), 90 * time.Second},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0},
	} {
		if got := parseRetryAfter(tc.in, now); got != tc.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestRetryAfterIsHonoured(t *testing.T) {
	f := newFixture(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	r := f.send(t, srv.URL)
	f.run(t, Config{AllowPrivateNetworks: true, Schedule: []time.Duration{time.Second}})

	d := f.waitFor(t, r.Message.ID, func(d postgres.Delivery) bool { return d.AttemptCount == 1 })
	if wait := time.Until(d.NextAttemptAt); wait < 59*time.Minute {
		t.Fatalf("next attempt in %v; the receiver asked for an hour", wait)
	}
}

func TestRotatedSecretSignsWithBoth(t *testing.T) {
	f := newFixture(t)
	oldSecret := f.secret
	newSecret := signing.NewSecret()
	var okOld, okNew atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		verify := func(secret string) bool {
			return signing.Verify(secret, r.Header.Get(signing.HeaderID), r.Header.Get(signing.HeaderTimestamp),
				r.Header.Get(signing.HeaderSignature), body, time.Now(), time.Minute) == nil
		}
		okOld.Store(verify(oldSecret))
		okNew.Store(verify(newSecret))
	}))
	defer srv.Close()

	r := f.send(t, srv.URL)
	ds, _ := f.store.ListDeliveries(context.Background(), f.app.ID, r.Message.ID, "", 100)
	if _, err := f.store.RotateSecret(context.Background(), f.app.ID, ds[0].EndpointID, newSecret, time.Hour); err != nil {
		t.Fatal(err)
	}
	f.run(t, Config{AllowPrivateNetworks: true})
	f.waitForState(t, r.Message.ID, postgres.DeliverySucceeded)
	if !okOld.Load() || !okNew.Load() {
		t.Fatalf("verifies with old secret: %v, new secret: %v; want both", okOld.Load(), okNew.Load())
	}
}

func TestBreakerStopsSendingToFailingEndpoint(t *testing.T) {
	f := newFixture(t)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	r := f.send(t, srv.URL)
	// Retries are immediate, so only the breaker stands between the
	// receiver and a request storm.
	f.run(t, Config{
		AllowPrivateNetworks: true,
		Schedule:             []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond},
		Health:               postgres.HealthPolicy{FailureThreshold: 3, CooldownBase: time.Hour},
	})
	f.waitFor(t, r.Message.ID, func(d postgres.Delivery) bool { return d.AttemptCount == 3 })
	time.Sleep(200 * time.Millisecond) // many poll intervals
	if n := calls.Load(); n != 3 {
		t.Fatalf("receiver called %d times; the breaker should stop it at 3", n)
	}
	ep, _ := f.store.GetEndpoint(context.Background(), f.app.ID, mustDelivery(t, f, r.Message.ID).EndpointID)
	if ep.CircuitOpenUntil == nil || time.Until(*ep.CircuitOpenUntil) < 50*time.Minute {
		t.Fatalf("circuit open until %v, want about an hour from now", ep.CircuitOpenUntil)
	}
}

func TestReplayRedeliversWithSameWebhookID(t *testing.T) {
	f := newFixture(t)
	var (
		healthy atomic.Bool
		ids     = make(chan string, 2)
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids <- r.Header.Get(signing.HeaderID)
		if !healthy.Load() {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	r := f.send(t, srv.URL)
	f.run(t, Config{AllowPrivateNetworks: true, Schedule: []time.Duration{}}) // first failure is final
	d := f.waitForState(t, r.Message.ID, postgres.DeliveryDead)

	healthy.Store(true)
	if _, err := f.store.ReplayDelivery(context.Background(), f.app.ID, d.ID); err != nil {
		t.Fatal(err)
	}
	f.waitForState(t, r.Message.ID, postgres.DeliverySucceeded)
	if first, second := <-ids, <-ids; first != r.Message.ID || second != first {
		t.Fatalf("webhook-ids = %q then %q, want %q both times", first, second, r.Message.ID)
	}
}

func TestShutdownFinishesSendsInProgress(t *testing.T) {
	f := newFixture(t)
	received := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(received)
		time.Sleep(300 * time.Millisecond)
	}))
	defer srv.Close()

	r := f.send(t, srv.URL)
	d := New(f.store, Config{AllowPrivateNetworks: true, PollInterval: 10 * time.Millisecond},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		d.Run(ctx)
		close(stopped)
	}()

	<-received
	cancel() // shut down while the request is in flight
	<-stopped
	// Run returned only after the send finished and was recorded.
	if got := mustDelivery(t, f, r.Message.ID); got.State != postgres.DeliverySucceeded {
		t.Fatalf("delivery after shutdown = %+v, want succeeded", got)
	}
}

func mustDelivery(t *testing.T, f fixture, msgID string) postgres.Delivery {
	t.Helper()
	ds, err := f.store.ListDeliveries(context.Background(), f.app.ID, msgID, "", 100)
	if err != nil || len(ds) != 1 {
		t.Fatalf("deliveries = %+v, %v", ds, err)
	}
	return ds[0]
}

// waitFor polls until the message's single delivery satisfies ok.
func (f fixture) waitFor(t *testing.T, msgID string, ok func(postgres.Delivery) bool) postgres.Delivery {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if d := mustDelivery(t, f, msgID); ok(d) {
			return d
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("delivery of %s never reached the expected state", msgID)
	return postgres.Delivery{}
}
