package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

func failJob(t *testing.T, s *Store, j Job) CompleteResult {
	t.Helper()
	r, err := s.Complete(context.Background(), j, Outcome{
		Attempt:   Attempt{AttemptedAt: time.Now(), LatencyMS: 1, Error: "boom"},
		NextState: DeliveryPending, RetryAt: time.Now(), // due again at once
	}, testPolicy)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func succeedJob(t *testing.T, s *Store, j Job) {
	t.Helper()
	if _, err := s.Complete(context.Background(), j, Outcome{
		Attempt: Attempt{AttemptedAt: time.Now(), LatencyMS: 1}, NextState: DeliverySucceeded,
	}, testPolicy); err != nil {
		t.Fatal(err)
	}
}

func exec(t *testing.T, s *Store, query string, args ...any) {
	t.Helper()
	if _, err := s.db.ExecContext(context.Background(), query, args...); err != nil {
		t.Fatal(err)
	}
}

func TestCircuitBreakerTripsProbesAndCloses(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	ep := mustEndpoint(t, s, app.ID)
	for range 5 {
		mustSend(t, s, app.ID, "a")
	}

	// testPolicy trips after 3 failures in a row.
	jobs, _ := s.Claim(ctx, 3, time.Minute)
	for i, j := range jobs {
		r := failJob(t, s, j)
		if r.Tripped != (i == 2) {
			t.Fatalf("failure %d: Tripped = %v", i+1, r.Tripped)
		}
	}
	e, _ := s.GetEndpoint(ctx, app.ID, ep.ID)
	if e.ConsecutiveFailures != 3 || e.CircuitOpenUntil == nil || e.FailingSince == nil {
		t.Fatalf("endpoint after trip = %+v", e)
	}
	if wait := time.Until(*e.CircuitOpenUntil); wait < 50*time.Second || wait > 70*time.Second {
		t.Fatalf("cool-down = %v, want about 1m", wait)
	}

	// Open: nothing is claimable, not even a probe, until the cool-down ends.
	if js, _ := s.Claim(ctx, 10, time.Minute); len(js) != 0 {
		t.Fatalf("claimed %d deliveries while the circuit is open", len(js))
	}
	if js, _ := s.ClaimProbes(ctx, 10, time.Minute); len(js) != 0 {
		t.Fatal("probed before the cool-down ended")
	}

	// Half-open: exactly one probe.
	exec(t, s, `UPDATE endpoints SET circuit_open_until = now() - interval '1 second'`)
	probes, err := s.ClaimProbes(ctx, 10, time.Minute)
	if err != nil || len(probes) != 1 || !probes[0].Probe {
		t.Fatalf("probes = %+v, %v; want one", probes, err)
	}
	if js, _ := s.ClaimProbes(ctx, 10, time.Minute); len(js) != 0 {
		t.Fatal("a second probe was claimed while the first is in flight")
	}

	// A failed probe re-opens the circuit for twice as long.
	failJob(t, s, probes[0])
	e, _ = s.GetEndpoint(ctx, app.ID, ep.ID)
	if wait := time.Until(*e.CircuitOpenUntil); wait < 110*time.Second || wait > 130*time.Second {
		t.Fatalf("cool-down after failed probe = %v, want about 2m", wait)
	}

	// A successful probe closes it and the backlog flows again.
	exec(t, s, `UPDATE endpoints SET circuit_open_until = now() - interval '1 second'`)
	probes, _ = s.ClaimProbes(ctx, 10, time.Minute)
	succeedJob(t, s, probes[0])
	e, _ = s.GetEndpoint(ctx, app.ID, ep.ID)
	if e.ConsecutiveFailures != 0 || e.CircuitOpenUntil != nil || e.FailingSince != nil {
		t.Fatalf("endpoint after successful probe = %+v", e)
	}
	if js, _ := s.Claim(ctx, 10, time.Minute); len(js) != 4 { // 5 minus the probe
		t.Fatalf("claimed %d after the circuit closed, want 4", len(js))
	}
}

func TestBreakerIsPerEndpoint(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	bad := mustEndpoint(t, s, app.ID)
	good := mustEndpoint(t, s, app.ID)
	for range 3 {
		mustSend(t, s, app.ID, "a")
	}
	jobs, _ := s.Claim(ctx, 10, time.Minute)
	for _, j := range jobs {
		if j.EndpointID == bad.ID {
			failJob(t, s, j)
		} else {
			succeedJob(t, s, j)
		}
	}
	mustSend(t, s, app.ID, "a")
	js, _ := s.Claim(ctx, 10, time.Minute)
	if len(js) != 1 || js[0].EndpointID != good.ID {
		t.Fatalf("claimed %+v; want only the healthy endpoint's delivery", js)
	}
}

func TestEndpointDisabledAfterContinuousFailure(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	ep := mustEndpoint(t, s, app.ID)
	mustSend(t, s, app.ID, "a")

	jobs, _ := s.Claim(ctx, 1, time.Minute)
	if r := failJob(t, s, jobs[0]); r.Disabled {
		t.Fatal("disabled on the first failure")
	}
	exec(t, s, `UPDATE endpoints SET failing_since = now() - interval '6 days', circuit_open_until = NULL`)
	jobs, _ = s.Claim(ctx, 1, time.Minute)
	if r := failJob(t, s, jobs[0]); !r.Disabled {
		t.Fatal("not disabled after 6 days of failure")
	}
	e, _ := s.GetEndpoint(ctx, app.ID, ep.ID)
	if e.Status != EndpointDisabled || e.DisabledAt == nil {
		t.Fatalf("endpoint = %+v", e)
	}
	if js, _ := s.Claim(ctx, 1, time.Minute); len(js) != 0 {
		t.Fatal("claimed a delivery for a disabled endpoint")
	}

	// Re-enabling clears the history so it starts healthy.
	active := EndpointActive
	e, err := s.UpdateEndpoint(ctx, app.ID, ep.ID, EndpointPatch{Status: &active})
	if err != nil || e.Status != EndpointActive || e.ConsecutiveFailures != 0 || e.FailingSince != nil || e.DisabledAt != nil {
		t.Fatalf("enabled endpoint = %+v, %v", e, err)
	}
	if js, _ := s.Claim(ctx, 1, time.Minute); len(js) != 1 {
		t.Fatal("delivery not claimable after re-enabling")
	}
}

func TestEndpointDisabledEvent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	failing := mustEndpoint(t, s, app.ID, "a")
	ops := mustEndpoint(t, s, app.ID, EventEndpointDisabled)
	mustEndpoint(t, s, app.ID) // empty filter: every operator event, but not the service's own
	mustSend(t, s, app.ID, "a")

	exec(t, s, `UPDATE endpoints SET failing_since = now() - interval '6 days' WHERE id = $1`, failing.ID)
	var job Job
	for {
		jobs, _ := s.Claim(ctx, 1, time.Minute)
		if len(jobs) == 0 {
			t.Fatal("the failing endpoint's delivery was never claimed")
		}
		if jobs[0].EndpointID == failing.ID {
			job = jobs[0]
			break
		}
		succeedJob(t, s, jobs[0]) // the wildcard endpoint's copy of "a"
	}
	r := failJob(t, s, job)
	if !r.Disabled || r.DisabledEvent == "" {
		t.Fatalf("result = %+v; want disabled with an event", r)
	}

	ds, err := s.ListDeliveries(ctx, app.ID, r.DisabledEvent, "", 100)
	if err != nil || len(ds) != 1 || ds[0].EndpointID != ops.ID {
		t.Fatalf("event deliveries = %+v, %v; want one, to the subscribed endpoint", ds, err)
	}
	var eventType, payloadEndpoint string
	if err := s.db.QueryRowContext(ctx, `SELECT event_type, payload->>'endpoint_id' FROM messages WHERE id = $1`,
		r.DisabledEvent).Scan(&eventType, &payloadEndpoint); err != nil {
		t.Fatal(err)
	}
	if eventType != EventEndpointDisabled || payloadEndpoint != failing.ID {
		t.Fatalf("event = %s about %s", eventType, payloadEndpoint)
	}
}

func TestRotateSecretSignsWithBothDuringOverlap(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	ep := mustEndpoint(t, s, app.ID)
	mustSend(t, s, app.ID, "a")

	const newSecret = "whsec_bmV3c2VjcmV0bmV3c2VjcmV0"
	if _, err := s.RotateSecret(ctx, app.ID, ep.ID, newSecret, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RotateSecret(ctx, "app_other", ep.ID, newSecret, time.Hour); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rotate via another app: %v", err)
	}
	jobs, _ := s.Claim(ctx, 1, time.Minute)
	if jobs[0].Secret != newSecret || jobs[0].PreviousSecret != ep.Secret {
		t.Fatalf("job secrets = %q / %q", jobs[0].Secret, jobs[0].PreviousSecret)
	}

	succeedJob(t, s, jobs[0])
	exec(t, s, `UPDATE endpoints SET previous_secret_expires_at = now() - interval '1 second'`)
	mustSend(t, s, app.ID, "a")
	jobs, _ = s.Claim(ctx, 1, time.Minute)
	if jobs[0].PreviousSecret != "" {
		t.Fatal("expired previous secret still used")
	}
}

func TestReplay(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	ep := mustEndpoint(t, s, app.ID)
	first := mustSend(t, s, app.ID, "a")
	mustSend(t, s, app.ID, "a")

	ds, _ := s.ListDeliveries(ctx, app.ID, first.Message.ID, "", 100)
	if _, err := s.ReplayDelivery(ctx, app.ID, ds[0].ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("replay of a pending delivery: %v, want ErrConflict", err)
	}
	if _, err := s.ReplayDelivery(ctx, app.ID, "del_missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replay of a missing delivery: %v", err)
	}
	other := mustApp(t, s)
	if _, err := s.ReplayDelivery(ctx, other.ID, ds[0].ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replay via another app: %v", err)
	}

	exec(t, s, `UPDATE deliveries SET state = 'dead', attempt_count = 10`)
	d, err := s.ReplayDelivery(ctx, app.ID, ds[0].ID)
	if err != nil || d.State != DeliveryPending || d.AttemptCount != 0 || d.MessageID != first.Message.ID {
		t.Fatalf("replayed = %+v, %v", d, err)
	}

	// Endpoint replay takes only dead deliveries created since the cut-off.
	exec(t, s, `UPDATE deliveries SET state = 'dead', created_at = now() - interval '2 days' WHERE id = $1`, ds[0].ID)
	n, err := s.ReplayEndpoint(ctx, app.ID, ep.ID, time.Now().Add(-24*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("ReplayEndpoint = %d, %v; want 1", n, err)
	}
	n, _ = s.ReplayEndpoint(ctx, app.ID, ep.ID, time.Now().Add(-72*time.Hour))
	if n != 1 { // the newer one is pending now; the older one is still dead
		t.Fatalf("second ReplayEndpoint = %d, want 1", n)
	}
	if _, err := s.ReplayEndpoint(ctx, other.ID, ep.ID, time.Time{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("endpoint replay via another app: %v", err)
	}
}
