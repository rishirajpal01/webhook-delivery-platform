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

// TestNoisyNeighbour is the phase 3 exit gate: one endpoint answers slowly
// and then fails, with a deep backlog; nine others are healthy. The healthy
// endpoints' p99 lag (from a delivery becoming due to its first attempt)
// must stay under one second.
func TestNoisyNeighbour(t *testing.T) {
	if testing.Short() {
		t.Skip("noisy neighbour test skipped in -short mode")
	}
	f := newFixture(t)
	ctx := context.Background()

	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second) // stands in for the doc's 10s, to keep the test short
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer slow.Close()
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

	endpoint(slow.URL, "noisy")
	for range 500 {
		send("noisy") // a backlog that, sent in order, would occupy every slot for minutes
	}
	for range 9 {
		endpoint(healthy.URL, "healthy")
	}

	f.run(t, Config{AllowPrivateNetworks: true, Concurrency: 16, RequestTimeout: 5 * time.Second})
	for range 60 { // 60 messages x 9 endpoints over 3 seconds
		send("healthy")
		time.Sleep(50 * time.Millisecond)
	}

	var lags []time.Duration
	deadline := time.Now().Add(15 * time.Second)
	for len(lags) < 540 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		lags = healthyLags(t, f)
	}
	if len(lags) < 540 {
		t.Fatalf("only %d of 540 healthy deliveries were attempted", len(lags))
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
