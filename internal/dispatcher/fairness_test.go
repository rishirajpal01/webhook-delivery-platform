package dispatcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
)

// TestNoisyNeighbour is the phase 3 exit gate from the design doc: one
// endpoint answers in 10 s or returns 500, with a deep backlog; nine others
// are healthy. The healthy endpoints' p99 lag (from a delivery being created
// to its first attempt) must stay under one second.
func TestNoisyNeighbour(t *testing.T) {
	if testing.Short() {
		t.Skip("noisy neighbour test skipped in -short mode")
	}
	tests := []struct {
		name  string
		noisy http.HandlerFunc
	}{
		{"answers in 10s", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(10 * time.Second):
			case <-r.Context().Done(): // the dispatcher stopped at the end of the test
			}
		}},
		{"returns 500", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { noisyNeighbour(t, tt.noisy) })
	}
}

func noisyNeighbour(t *testing.T, noisyHandler http.HandlerFunc) {
	f := newFixture(t)
	ctx := context.Background()

	noisy := httptest.NewServer(noisyHandler)
	defer noisy.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer healthy.Close()

	endpoint := func(url, eventType string) {
		t.Helper()
		if _, err := f.store.CreateEndpoint(ctx, postgres.Endpoint{
			AppID: f.app.ID, URL: url, Secret: f.secret, EventTypes: []string{eventType},
		}); err != nil {
			t.Fatal(err)
		}
	}
	send := func(eventType string) {
		t.Helper()
		if _, err := f.store.CreateMessage(ctx, postgres.Message{
			AppID: f.app.ID, EventType: eventType, Payload: json.RawMessage(`{}`),
		}); err != nil {
			t.Fatal(err)
		}
	}

	endpoint(noisy.URL, "noisy")
	for range 500 {
		send("noisy") // a backlog that, sent in order, would occupy every slot for minutes
	}
	for range 9 {
		endpoint(healthy.URL, "healthy")
	}

	// The default 15s request timeout, so a 10s answer is waited for.
	f.run(t, Config{AllowPrivateNetworks: true, Concurrency: 16})
	// 30 messages x 9 endpoints over 3 seconds: 90 deliveries/s, well under
	// what the 12 slots the noisy endpoint can't take will carry even on a slow
	// CI runner under -race. The test checks isolation, not throughput: run
	// near saturation, queueing alone pushes p99 past the limit.
	for range 30 {
		send("healthy")
		time.Sleep(100 * time.Millisecond)
	}

	var lags []time.Duration
	deadline := time.Now().Add(15 * time.Second)
	for len(lags) < 270 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		lags = healthyLags(t, f)
	}
	if len(lags) < 270 {
		t.Fatalf("only %d of 270 healthy deliveries were attempted", len(lags))
	}
	slices.Sort(lags)
	p50, p99 := lags[len(lags)/2], lags[len(lags)*99/100]
	t.Logf("healthy endpoints: p50 lag %v, p99 lag %v over %d deliveries", p50, p99, len(lags))
	if p99 > time.Second {
		t.Fatalf("healthy p99 lag %v, want under 1s", p99)
	}
}

// healthyLags returns, for each delivery of a "healthy" message that has been
// attempted, the time from its creation to its first attempt.
func healthyLags(t *testing.T, f fixture) []time.Duration {
	t.Helper()
	rows, err := f.db.Query(`
		SELECT extract(epoch FROM min(a.attempted_at) - d.created_at)
		FROM deliveries d
		JOIN messages m ON m.id = d.message_id
		JOIN attempts a ON a.delivery_id = d.id
		WHERE m.event_type = 'healthy'
		GROUP BY d.id, d.created_at`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var lags []time.Duration
	for rows.Next() {
		var secs float64
		if err := rows.Scan(&secs); err != nil {
			t.Fatal(err)
		}
		lags = append(lags, time.Duration(secs*float64(time.Second)))
	}
	return lags
}
