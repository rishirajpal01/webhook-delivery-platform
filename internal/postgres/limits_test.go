package postgres

import (
	"context"
	"testing"
	"time"
)

func limitedEndpoint(t *testing.T, s *Store, appID string, maxInFlight, rateLimit int) Endpoint {
	t.Helper()
	e, err := s.CreateEndpoint(context.Background(), Endpoint{
		AppID: appID, URL: "https://example.com/hook", Secret: "whsec_dGVzdHRlc3R0ZXN0dGVzdA==",
		MaxInFlight: maxInFlight, RateLimit: rateLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestClaimRespectsConcurrencyCap(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	ep := limitedEndpoint(t, s, app.ID, 2, 0)
	if ep.MaxInFlight != 2 || ep.RateLimit != 0 {
		t.Fatalf("endpoint = %+v", ep)
	}
	for range 5 {
		mustSend(t, s, app.ID, "a")
	}

	jobs, _ := s.Claim(ctx, 10, time.Minute)
	if len(jobs) != 2 {
		t.Fatalf("claimed %d, want the cap of 2", len(jobs))
	}
	if more, _ := s.Claim(ctx, 10, time.Minute); len(more) != 0 {
		t.Fatalf("claimed %d more while at the cap", len(more))
	}
	succeedJob(t, s, jobs[0])
	if more, _ := s.Claim(ctx, 10, time.Minute); len(more) != 1 {
		t.Fatalf("claimed %d after one finished, want 1", len(more))
	}
}

func TestDefaultConcurrencyCap(t *testing.T) {
	s := testStore(t)
	app := mustApp(t, s)
	if ep := mustEndpoint(t, s, app.ID); ep.MaxInFlight != 4 {
		t.Fatalf("default max_in_flight = %d, want 4", ep.MaxInFlight)
	}
}

func TestBacklogDoesNotBlockOtherEndpoints(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	noisy := limitedEndpoint(t, s, app.ID, 100, 0)
	for range 20 {
		mustSend(t, s, app.ID, "a")
	}
	// The healthy endpoint joins after the backlog, so all of its work is
	// newer than the noisy endpoint's.
	quiet := limitedEndpoint(t, s, app.ID, 100, 0)
	exec(t, s, `UPDATE deliveries SET next_attempt_at = now() - interval '1 hour'`)
	mustSend(t, s, app.ID, "a")

	jobs, _ := s.Claim(ctx, 4, time.Minute)
	per := map[string]int{}
	for _, j := range jobs {
		per[j.EndpointID]++
	}
	if len(jobs) != 4 || per[quiet.ID] != 1 || per[noisy.ID] != 3 {
		t.Fatalf("claimed %v; want the quiet endpoint's delivery among them", per)
	}
}

func TestClaimRespectsRateLimit(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	limitedEndpoint(t, s, app.ID, 100, 2) // 2 per second
	for range 10 {
		mustSend(t, s, app.ID, "a")
	}

	if jobs, _ := s.Claim(ctx, 10, time.Minute); len(jobs) != 2 {
		t.Fatalf("first claim got %d, want a burst of 2", len(jobs))
	}
	if jobs, _ := s.Claim(ctx, 10, time.Minute); len(jobs) != 0 {
		t.Fatalf("claimed %d with no tokens left", len(jobs))
	}
	time.Sleep(600 * time.Millisecond) // one token refills every 500ms
	if jobs, _ := s.Claim(ctx, 10, time.Minute); len(jobs) != 1 {
		t.Fatalf("claimed %d after 600ms, want 1", len(jobs))
	}
}

func TestUpdateEndpointLimits(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	app := mustApp(t, s)
	ep := limitedEndpoint(t, s, app.ID, 2, 5)

	eight, zero := 8, 0
	e, err := s.UpdateEndpoint(ctx, app.ID, ep.ID, EndpointPatch{MaxInFlight: &eight})
	if err != nil || e.MaxInFlight != 8 || e.RateLimit != 5 {
		t.Fatalf("after setting max_in_flight: %+v, %v", e, err)
	}
	e, err = s.UpdateEndpoint(ctx, app.ID, ep.ID, EndpointPatch{RateLimit: &zero})
	if err != nil || e.MaxInFlight != 8 || e.RateLimit != 0 {
		t.Fatalf("after removing the rate limit: %+v, %v", e, err)
	}
}
