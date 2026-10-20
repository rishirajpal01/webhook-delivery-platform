package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
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

func TestCreateMessageFansOutToMatchingEndpoints(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)

	all := mustEndpoint(t, s, app.ID)                        // every event type
	match := mustEndpoint(t, s, app.ID, "order.paid", "x.y") // subscribed
	mustEndpoint(t, s, app.ID, "order.refunded")             // not subscribed
	paused := mustEndpoint(t, s, app.ID)                     // paused below: deliveries are held
	disabled := mustEndpoint(t, s, app.ID)                   // disabled below: gets none
	for id, status := range map[string]string{paused.ID: EndpointPaused, disabled.ID: EndpointDisabled} {
		if _, err := s.UpdateEndpoint(ctx, app.ID, id, EndpointPatch{Status: &status}); err != nil {
			t.Fatal(err)
		}
	}

	r := mustSend(t, s, app.ID, "order.paid")
	if r.Deliveries != 3 || r.Duplicate {
		t.Fatalf("send = %+v, want 3 deliveries", r)
	}
	ds, err := s.ListDeliveries(ctx, app.ID, r.Message.ID, "", 100)
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
	if len(got) != 3 || !got[all.ID] || !got[match.ID] || !got[paused.ID] {
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

	// The same key with another payload or event type is refused.
	for _, m := range []Message{
		{AppID: app.ID, EventType: "order.paid", Payload: json.RawMessage(`{"a":2}`), IdempotencyKey: "k1"},
		{AppID: app.ID, EventType: "order.refunded", Payload: json.RawMessage(`{"a":1}`), IdempotencyKey: "k1"},
	} {
		if _, err := s.CreateMessage(ctx, m); !errors.Is(err, ErrIdempotencyMismatch) {
			t.Errorf("reused key with %s %s: err %v, want ErrIdempotencyMismatch", m.EventType, m.Payload, err)
		}
	}
	// Equal JSON written differently is the same request.
	same := msg
	same.Payload = json.RawMessage(`{ "a" : 1 }`)
	if r, err := s.CreateMessage(ctx, same); err != nil || !r.Duplicate {
		t.Fatalf("reformatted payload = %+v, %v; want the first message", r, err)
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
	ds, _ := s.ListDeliveries(ctx, app.ID, r.Message.ID, "", 100)
	if ds[0].State != DeliveryPending || ds[0].AttemptCount != 1 {
		t.Fatalf("after retry: %+v", ds[0])
	}
	as, err := s.ListAttempts(ctx, app.ID, j.DeliveryID, 0, 100)
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
	ds, _ := s.ListDeliveries(ctx, app.ID, r.Message.ID, "", 100)
	if ds[0].State != DeliveryInFlight {
		t.Fatalf("state = %s, want in_flight", ds[0].State)
	}

	res, err = s.Complete(ctx, fresh[0], Outcome{
		Attempt: Attempt{AttemptedAt: time.Now(), LatencyMS: 1}, NextState: DeliverySucceeded,
	}, testPolicy)
	if err != nil || !res.Applied {
		t.Fatalf("fresh Complete = %+v, %v", res, err)
	}
	as, _ := s.ListAttempts(ctx, app.ID, fresh[0].DeliveryID, 0, 100)
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

func TestIdempotentCreates(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	a1, dup, err := s.CreateAppIdempotent(ctx, "acme", "app-key")
	if err != nil || dup {
		t.Fatalf("first app create: dup %v, err %v", dup, err)
	}
	a2, dup, err := s.CreateAppIdempotent(ctx, "acme", "app-key")
	if err != nil || !dup || a2.ID != a1.ID {
		t.Fatalf("repeat app create = %+v, dup %v, err %v; want the first app", a2, dup, err)
	}

	ep := Endpoint{AppID: a1.ID, URL: "https://example.com/hook", Secret: "whsec_dGVzdHRlc3R0ZXN0dGVzdA=="}
	e1, dup, err := s.CreateEndpointIdempotent(ctx, ep, "ep-key")
	if err != nil || dup {
		t.Fatalf("first endpoint create: dup %v, err %v", dup, err)
	}
	e2, dup, err := s.CreateEndpointIdempotent(ctx, ep, "ep-key")
	if err != nil || !dup || e2.ID != e1.ID || e2.Secret != ep.Secret {
		t.Fatalf("repeat endpoint create = %+v, dup %v, err %v; want the first endpoint with its secret", e2, dup, err)
	}
	if eps, _ := s.ListEndpoints(ctx, a1.ID, "", 10); len(eps) != 1 {
		t.Fatalf("%d endpoints, want 1", len(eps))
	}
	// A retry stays the same request after the endpoint is changed, and a
	// generated secret is not part of it; another URL is a different one.
	newURL := "https://example.com/moved"
	if _, err := s.UpdateEndpoint(ctx, a1.ID, e1.ID, EndpointPatch{URL: &newURL}); err != nil {
		t.Fatal(err)
	}
	retry := ep
	retry.Secret = "whsec_b3RoZXJvdGhlcm90aGVyIQ=="
	if _, dup, err := s.CreateEndpointIdempotent(ctx, retry, "ep-key"); err != nil || !dup {
		t.Fatalf("retry after update: dup %v, err %v", dup, err)
	}
	retry.URL = newURL
	if _, _, err := s.CreateEndpointIdempotent(ctx, retry, "ep-key"); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("other URL: err %v, want ErrIdempotencyMismatch", err)
	}
	if _, _, err := s.CreateAppIdempotent(ctx, "globex", "app-key"); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("other app name: err %v, want ErrIdempotencyMismatch", err)
	}
	// Keys are per app, and a missing app is still not found.
	other := mustApp(t, s)
	ep.AppID = other.ID
	if _, dup, err := s.CreateEndpointIdempotent(ctx, ep, "ep-key"); err != nil || dup {
		t.Fatalf("same key in another app: dup %v, err %v", dup, err)
	}
	ep.AppID = "app_missing"
	if _, _, err := s.CreateEndpointIdempotent(ctx, ep, "ep-key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing app: err %v, want ErrNotFound", err)
	}
}

func TestDeliveryAndAttemptPages(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	for range 5 {
		mustEndpoint(t, s, app.ID)
	}
	r := mustSend(t, s, app.ID, "a")

	var seen []string
	cursor := ""
	for {
		page, err := s.ListDeliveries(ctx, app.ID, r.Message.ID, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range page {
			seen = append(seen, d.ID)
		}
		if len(page) < 2 {
			break
		}
		cursor = page[len(page)-1].ID
	}
	if len(seen) != 5 || !slices.IsSorted(seen) {
		t.Fatalf("paged deliveries = %v; want 5 in id order", seen)
	}

	// Each Complete logs an attempt, even once the lease is gone.
	jobs, _ := s.Claim(ctx, 1, time.Minute)
	j := jobs[0]
	for range 3 {
		if _, err := s.Complete(ctx, j, Outcome{
			Attempt:   Attempt{AttemptedAt: time.Now(), LatencyMS: 1, Error: "boom"},
			NextState: DeliveryPending, RetryAt: time.Now(),
		}, HealthPolicy{FailureThreshold: 100, CooldownBase: time.Minute, CooldownMax: time.Hour, DisableAfter: time.Hour}); err != nil {
			t.Fatal(err)
		}
	}
	first, _ := s.ListAttempts(ctx, app.ID, j.DeliveryID, 0, 2)
	rest, _ := s.ListAttempts(ctx, app.ID, j.DeliveryID, first[1].ID, 2)
	if len(first) != 2 || len(rest) != 1 || rest[0].ID <= first[1].ID {
		t.Fatalf("attempt pages = %+v / %+v", first, rest)
	}
}
