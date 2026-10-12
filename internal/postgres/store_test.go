package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

var testPolicy = HealthPolicy{
	FailureThreshold: 3,
	CooldownBase:     time.Minute,
	CooldownMax:      time.Hour,
	DisableAfter:     5 * 24 * time.Hour,
}

func testStore(t *testing.T) *Store {
	t.Helper()
	db := testDB(t)
	if _, err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return NewStore(db)
}

func mustApp(t *testing.T, s *Store) App {
	t.Helper()
	a, err := s.CreateApp(context.Background(), "acme")
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mustEndpoint(t *testing.T, s *Store, appID string, eventTypes ...string) Endpoint {
	t.Helper()
	e, err := s.CreateEndpoint(context.Background(), Endpoint{
		AppID: appID, URL: "https://example.com/hook", Secret: "whsec_dGVzdHRlc3R0ZXN0dGVzdA==", EventTypes: eventTypes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func mustSend(t *testing.T, s *Store, appID, eventType string) SendResult {
	t.Helper()
	r, err := s.CreateMessage(context.Background(), Message{
		AppID: appID, EventType: eventType, Payload: json.RawMessage(`{"n":1}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestAppsAndEndpoints(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	if _, err := s.GetApp(ctx, "app_missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetApp missing: %v", err)
	}
	app := mustApp(t, s)
	if got, err := s.GetApp(ctx, app.ID); err != nil || got.Name != "acme" {
		t.Fatalf("GetApp = %+v, %v", got, err)
	}

	if _, err := s.CreateEndpoint(ctx, Endpoint{AppID: "app_missing", URL: "https://x", Secret: "s"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("CreateEndpoint for missing app: %v", err)
	}

	var ids []string
	for range 3 {
		ids = append(ids, mustEndpoint(t, s, app.ID, "order.paid").ID)
	}
	first, err := s.ListEndpoints(ctx, app.ID, "", 2)
	if err != nil || len(first) != 2 {
		t.Fatalf("page 1 = %d endpoints, %v", len(first), err)
	}
	second, err := s.ListEndpoints(ctx, app.ID, first[1].ID, 2)
	if err != nil || len(second) != 1 {
		t.Fatalf("page 2 = %d endpoints, %v", len(second), err)
	}

	paused, url := EndpointPaused, "https://example.org/new"
	e, err := s.UpdateEndpoint(ctx, app.ID, ids[0], EndpointPatch{Status: &paused, URL: &url})
	if err != nil || e.Status != EndpointPaused || e.URL != url || len(e.EventTypes) != 1 {
		t.Fatalf("UpdateEndpoint = %+v, %v", e, err)
	}

	other := mustApp(t, s)
	if _, err := s.GetEndpoint(ctx, other.ID, ids[0]); !errors.Is(err, ErrNotFound) {
		t.Fatalf("endpoint visible from another app: %v", err)
	}
	if err := s.DeleteEndpoint(ctx, app.ID, ids[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteEndpoint(ctx, app.ID, ids[0]); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}

func TestCreateMessageFansOutToMatchingActiveEndpoints(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)

	all := mustEndpoint(t, s, app.ID)                        // every event type
	match := mustEndpoint(t, s, app.ID, "order.paid", "x.y") // subscribed
	mustEndpoint(t, s, app.ID, "order.refunded")             // not subscribed
	paused := mustEndpoint(t, s, app.ID)                     // paused below
	p := EndpointPaused
	if _, err := s.UpdateEndpoint(ctx, app.ID, paused.ID, EndpointPatch{Status: &p}); err != nil {
		t.Fatal(err)
	}

	r := mustSend(t, s, app.ID, "order.paid")
	if r.Deliveries != 2 || r.Duplicate {
		t.Fatalf("send = %+v, want 2 deliveries", r)
	}
	ds, err := s.ListDeliveries(ctx, app.ID, r.Message.ID)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, d := range ds {
		got[d.EndpointID] = true
		if d.State != DeliveryPending {
			t.Errorf("delivery state = %s", d.State)
		}
	}
	if len(got) != 2 || !got[all.ID] || !got[match.ID] {
		t.Fatalf("deliveries went to %v", got)
	}

	if _, err := s.CreateMessage(ctx, Message{AppID: "app_missing", EventType: "a", Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("send to missing app: %v", err)
	}
}

func TestCreateMessageIdempotency(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	mustEndpoint(t, s, app.ID)

	msg := Message{AppID: app.ID, EventType: "order.paid", Payload: json.RawMessage(`{"a":1}`), IdempotencyKey: "k1"}

	// Concurrent sends with one key create exactly one message.
	const senders = 8
	results := make([]SendResult, senders)
	var wg sync.WaitGroup
	for i := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := s.CreateMessage(ctx, msg)
			if err != nil {
				t.Error(err)
			}
			results[i] = r
		}()
	}
	wg.Wait()

	originals := 0
	for _, r := range results {
		if r.Message.ID != results[0].Message.ID {
			t.Fatalf("different message ids for one key: %s vs %s", r.Message.ID, results[0].Message.ID)
		}
		if r.Deliveries != 1 {
			t.Errorf("deliveries = %d, want 1", r.Deliveries)
		}
		if !r.Duplicate {
			originals++
		}
	}
	if originals != 1 {
		t.Fatalf("%d sends created a message, want 1", originals)
	}

	// The same key in another app is independent.
	other := mustApp(t, s)
	msg.AppID = other.ID
	if r, err := s.CreateMessage(ctx, msg); err != nil || r.Duplicate {
		t.Fatalf("other app send = %+v, %v", r, err)
	}
}

func TestClaimLeasesDueDeliveriesOnce(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	ep := mustEndpoint(t, s, app.ID)
	r := mustSend(t, s, app.ID, "a.b")

	jobs, err := s.Claim(ctx, 10, time.Minute)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Claim = %d jobs, %v", len(jobs), err)
	}
	j := jobs[0]
	if j.MessageID != r.Message.ID || j.EndpointID != ep.ID || j.URL != ep.URL || j.Secret != ep.Secret ||
		j.EventType != "a.b" || string(j.Payload) != `{"n": 1}` {
		t.Fatalf("job = %+v", j)
	}
	if again, _ := s.Claim(ctx, 10, time.Minute); len(again) != 0 {
		t.Fatalf("claimed an in-flight delivery again: %+v", again)
	}

	// Retry later: not due yet, so not claimable.
	res, err := s.Complete(ctx, j, Outcome{
		Attempt:   Attempt{AttemptedAt: time.Now(), LatencyMS: 5, Error: "boom"},
		NextState: DeliveryPending, RetryAt: time.Now().Add(time.Hour),
	}, testPolicy)
	if err != nil || !res.Applied {
		t.Fatalf("Complete = %+v, %v", res, err)
	}
	if again, _ := s.Claim(ctx, 10, time.Minute); len(again) != 0 {
		t.Fatal("claimed a delivery that is not due")
	}
	ds, _ := s.ListDeliveries(ctx, app.ID, r.Message.ID)
	if ds[0].State != DeliveryPending || ds[0].AttemptCount != 1 {
		t.Fatalf("after retry: %+v", ds[0])
	}
	as, err := s.ListAttempts(ctx, app.ID, j.DeliveryID)
	if err != nil || len(as) != 1 || as[0].Error != "boom" || as[0].StatusCode != nil {
		t.Fatalf("attempts = %+v, %v", as, err)
	}
}

func TestClaimSkipsPausedEndpoints(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	ep := mustEndpoint(t, s, app.ID)
	mustSend(t, s, app.ID, "a")

	p := EndpointPaused
	s.UpdateEndpoint(ctx, app.ID, ep.ID, EndpointPatch{Status: &p})
	if jobs, _ := s.Claim(ctx, 10, time.Minute); len(jobs) != 0 {
		t.Fatal("claimed a delivery for a paused endpoint")
	}
	a := EndpointActive
	s.UpdateEndpoint(ctx, app.ID, ep.ID, EndpointPatch{Status: &a})
	if jobs, _ := s.Claim(ctx, 10, time.Minute); len(jobs) != 1 {
		t.Fatal("delivery not claimable after resuming the endpoint")
	}
}

func TestExpiredLeaseIsReleasedAndStaleCompletionIgnored(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	mustEndpoint(t, s, app.ID)
	r := mustSend(t, s, app.ID, "a")

	stale, _ := s.Claim(ctx, 1, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if n, err := s.ReleaseExpired(ctx); err != nil || n != 1 {
		t.Fatalf("ReleaseExpired = %d, %v", n, err)
	}
	fresh, _ := s.Claim(ctx, 1, time.Minute)
	if len(fresh) != 1 {
		t.Fatal("released delivery was not claimable")
	}

	// The first dispatcher finishes late: its attempt is logged, but the row
	// belongs to the second claim now.
	res, err := s.Complete(ctx, stale[0], Outcome{
		Attempt: Attempt{AttemptedAt: time.Now(), LatencyMS: 1}, NextState: DeliverySucceeded,
	}, testPolicy)
	if err != nil || res.Applied {
		t.Fatalf("stale Complete = %+v, %v; want no state change", res, err)
	}
	ds, _ := s.ListDeliveries(ctx, app.ID, r.Message.ID)
	if ds[0].State != DeliveryInFlight {
		t.Fatalf("state = %s, want in_flight", ds[0].State)
	}

	res, err = s.Complete(ctx, fresh[0], Outcome{
		Attempt: Attempt{AttemptedAt: time.Now(), LatencyMS: 1}, NextState: DeliverySucceeded,
	}, testPolicy)
	if err != nil || !res.Applied {
		t.Fatalf("fresh Complete = %+v, %v", res, err)
	}
	as, _ := s.ListAttempts(ctx, app.ID, fresh[0].DeliveryID)
	if len(as) != 2 {
		t.Fatalf("attempts = %d, want 2 (both calls happened)", len(as))
	}
}

func TestConcurrentClaimersNeverShareADelivery(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	for range 4 {
		// Room for every delivery: this test is about claims, not caps.
		if _, err := s.CreateEndpoint(ctx, Endpoint{AppID: app.ID, URL: "https://x.io", Secret: "s", MaxInFlight: 100}); err != nil {
			t.Fatal(err)
		}
	}
	for range 50 {
		mustSend(t, s, app.ID, "a") // 50 messages x 4 endpoints = 200 deliveries
	}

	var (
		mu   sync.Mutex
		seen = map[string]int{}
		wg   sync.WaitGroup
	)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				jobs, err := s.Claim(ctx, 7, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					seen[j.DeliveryID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if len(seen) != 200 {
		t.Fatalf("claimed %d distinct deliveries, want 200", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("delivery %s claimed %d times", id, n)
		}
	}
}
