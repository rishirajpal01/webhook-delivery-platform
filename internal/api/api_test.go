package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/testdb"
)

const testKey = "test-key"

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type fakePinger struct{ err error }

func (f fakePinger) PingContext(context.Context) error { return f.err }

func TestHealthRoutes(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
		dbErr  error
		want   int
	}{
		{"healthz", "GET", "/healthz", nil, http.StatusOK},
		{"healthz ignores the database", "GET", "/healthz", errors.New("down"), http.StatusOK},
		{"readyz with database up", "GET", "/readyz", nil, http.StatusOK},
		{"readyz with database down", "GET", "/readyz", errors.New("down"), http.StatusServiceUnavailable},
		{"metrics", "GET", "/metrics", nil, http.StatusOK},
		{"wrong method", "POST", "/healthz", nil, http.StatusMethodNotAllowed},
		{"unknown path", "GET", "/nope", nil, http.StatusNotFound},
		{"v1 without key", "GET", "/v1/apps/app_1", nil, http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(fakePinger{tt.dbErr}, nil, sha256.Sum256([]byte(testKey)), discard)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

// client calls the handler with the API key.
type client struct {
	t  *testing.T
	h  http.Handler
	db *sql.DB // for setting up states the API cannot reach directly
}

func newClient(t *testing.T) client {
	db, _ := testdb.New(t)
	if _, err := postgres.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return client{t: t, h: NewHandler(db, postgres.NewStore(db), sha256.Sum256([]byte(testKey)), discard), db: db}
}

// do sends a request and decodes the JSON response into out (if non-nil).
func (c client) do(method, path, body string, out any, headers ...string) *httptest.ResponseRecorder {
	c.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testKey)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	c.h.ServeHTTP(rec, req)
	if out != nil {
		if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
			c.t.Fatalf("%s %s: decode %q: %v", method, path, rec.Body.String(), err)
		}
	}
	return rec
}

func (c client) expect(rec *httptest.ResponseRecorder, want int) {
	c.t.Helper()
	if rec.Code != want {
		c.t.Fatalf("status = %d, want %d; body %s", rec.Code, want, rec.Body.String())
	}
}

func TestSendFlow(t *testing.T) {
	c := newClient(t)

	var app postgres.App
	c.expect(c.do("POST", "/v1/apps", `{"name":"acme"}`, &app), http.StatusCreated)
	base := "/v1/apps/" + app.ID

	var ep postgres.Endpoint
	c.expect(c.do("POST", base+"/endpoints", `{"url":"https://example.com/hook","event_types":["order.paid"]}`, &ep), http.StatusCreated)
	if !strings.HasPrefix(ep.Secret, "whsec_") {
		t.Fatalf("create should return a generated secret, got %q", ep.Secret)
	}
	var got postgres.Endpoint
	c.expect(c.do("GET", base+"/endpoints/"+ep.ID, "", &got), http.StatusOK)
	if got.Secret != "" {
		t.Fatal("GET must not return the secret")
	}

	var sent struct {
		ID         string `json:"id"`
		Deliveries int    `json:"deliveries"`
	}
	msg := `{"event_type":"order.paid","payload":{"order_id":"981"}}`
	c.expect(c.do("POST", base+"/messages", msg, &sent, "Idempotency-Key", "order-981"), http.StatusAccepted)
	if sent.Deliveries != 1 {
		t.Fatalf("deliveries = %d, want 1", sent.Deliveries)
	}

	var again struct{ ID string }
	rec := c.do("POST", base+"/messages", msg, &again, "Idempotency-Key", "order-981")
	c.expect(rec, http.StatusOK)
	if again.ID != sent.ID || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("repeat send returned %s (replayed=%q), want %s", again.ID, rec.Header().Get("Idempotent-Replayed"), sent.ID)
	}

	var ds list[postgres.Delivery]
	c.expect(c.do("GET", base+"/messages/"+sent.ID+"/deliveries", "", &ds), http.StatusOK)
	if len(ds.Data) != 1 || ds.Data[0].EndpointID != ep.ID || ds.Data[0].State != "pending" {
		t.Fatalf("deliveries = %+v", ds.Data)
	}
	var as list[postgres.Attempt]
	c.expect(c.do("GET", base+"/deliveries/"+ds.Data[0].ID+"/attempts", "", &as), http.StatusOK)
	if len(as.Data) != 0 {
		t.Fatalf("attempts = %+v, want none yet", as.Data)
	}

	if ep.MaxInFlight != 4 || ep.RateLimit != 0 {
		t.Fatalf("default limits = %d in flight, %d/s", ep.MaxInFlight, ep.RateLimit)
	}
	var patched postgres.Endpoint
	c.expect(c.do("PATCH", base+"/endpoints/"+ep.ID, `{"status":"paused","max_in_flight":10,"rate_limit":50}`, &patched), http.StatusOK)
	if patched.Status != "paused" || len(patched.EventTypes) != 1 || patched.MaxInFlight != 10 || patched.RateLimit != 50 {
		t.Fatalf("patched = %+v", patched)
	}

	var page list[postgres.Endpoint]
	c.expect(c.do("GET", base+"/endpoints?limit=1", "", &page), http.StatusOK)
	if len(page.Data) != 1 || page.NextCursor != ep.ID || page.Data[0].Secret != "" {
		t.Fatalf("page = %+v", page)
	}
	var page2 list[postgres.Endpoint]
	c.expect(c.do("GET", base+"/endpoints?limit=1&cursor="+page.NextCursor, "", &page2), http.StatusOK)
	if len(page2.Data) != 0 || page2.NextCursor != "" {
		t.Fatalf("second page = %+v", page2)
	}

	c.expect(c.do("DELETE", base+"/endpoints/"+ep.ID, "", nil), http.StatusNoContent)
	c.expect(c.do("GET", base+"/endpoints/"+ep.ID, "", nil), http.StatusNotFound)
}

func TestIdempotentCreatesAndPages(t *testing.T) {
	c := newClient(t)

	var app, again postgres.App
	c.expect(c.do("POST", "/v1/apps", `{"name":"acme"}`, &app, "Idempotency-Key", "k1"), http.StatusCreated)
	rec := c.do("POST", "/v1/apps", `{"name":"acme"}`, &again, "Idempotency-Key", "k1")
	c.expect(rec, http.StatusOK)
	if again.ID != app.ID || rec.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("repeat app create = %+v (replayed=%q), want %s", again, rec.Header().Get("Idempotent-Replayed"), app.ID)
	}
	base := "/v1/apps/" + app.ID

	var ep, epAgain postgres.Endpoint
	body := `{"url":"https://example.com/hook"}`
	c.expect(c.do("POST", base+"/endpoints", body, &ep, "Idempotency-Key", "k1"), http.StatusCreated)
	c.expect(c.do("POST", base+"/endpoints", body, &epAgain, "Idempotency-Key", "k1"), http.StatusOK)
	if epAgain.ID != ep.ID || epAgain.Secret != ep.Secret {
		t.Fatalf("repeat endpoint create = %+v, want %+v", epAgain, ep)
	}
	for range 2 {
		c.expect(c.do("POST", base+"/endpoints", body, nil), http.StatusCreated)
	}

	// Three deliveries, read two at a time.
	var sent struct{ ID string }
	c.expect(c.do("POST", base+"/messages", `{"event_type":"a","payload":{}}`, &sent), http.StatusAccepted)
	var p1, p2 list[postgres.Delivery]
	c.expect(c.do("GET", base+"/messages/"+sent.ID+"/deliveries?limit=2", "", &p1), http.StatusOK)
	c.expect(c.do("GET", base+"/messages/"+sent.ID+"/deliveries?limit=2&cursor="+p1.NextCursor, "", &p2), http.StatusOK)
	if len(p1.Data) != 2 || p1.NextCursor == "" || len(p2.Data) != 1 || p2.NextCursor != "" || p2.Data[0].ID <= p1.NextCursor {
		t.Fatalf("delivery pages = %+v / %+v", p1, p2)
	}

	// Attempts page the same way.
	del := p2.Data[0].ID
	if _, err := c.db.Exec(`INSERT INTO attempts (delivery_id, latency_ms) SELECT $1, 1 FROM generate_series(1, 3)`, del); err != nil {
		t.Fatal(err)
	}
	var a1, a2 list[postgres.Attempt]
	c.expect(c.do("GET", base+"/deliveries/"+del+"/attempts?limit=2", "", &a1), http.StatusOK)
	c.expect(c.do("GET", base+"/deliveries/"+del+"/attempts?limit=2&cursor="+a1.NextCursor, "", &a2), http.StatusOK)
	if len(a1.Data) != 2 || len(a2.Data) != 1 || a2.NextCursor != "" || a2.Data[0].ID <= a1.Data[1].ID {
		t.Fatalf("attempt pages = %+v / %+v", a1, a2)
	}
}

func TestValidationAndNotFound(t *testing.T) {
	c := newClient(t)
	var app postgres.App
	c.expect(c.do("POST", "/v1/apps", `{"name":"acme"}`, &app), http.StatusCreated)
	base := "/v1/apps/" + app.ID

	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"unknown field", "POST", "/v1/apps", `{"name":"a","x":1}`, 400},
		{"bad json", "POST", "/v1/apps", `{`, 400},
		{"empty name", "POST", "/v1/apps", `{"name":""}`, 422},
		{"missing app", "GET", "/v1/apps/app_missing", "", 404},
		{"endpoint for missing app", "POST", "/v1/apps/app_missing/endpoints", `{"url":"https://x.io"}`, 404},
		{"ftp url", "POST", base + "/endpoints", `{"url":"ftp://x.io"}`, 422},
		{"relative url", "POST", base + "/endpoints", `{"url":"/hook"}`, 422},
		{"url with credentials", "POST", base + "/endpoints", `{"url":"https://u:p@x.io"}`, 422},
		{"bad secret", "POST", base + "/endpoints", `{"url":"https://x.io","secret":"hunter2"}`, 422},
		{"empty event type", "POST", base + "/endpoints", `{"url":"https://x.io","event_types":[""]}`, 422},
		{"set disabled", "PATCH", base + "/endpoints/ep_x", `{"status":"disabled"}`, 422},
		{"patch missing endpoint", "PATCH", base + "/endpoints/ep_x", `{"status":"paused"}`, 404},
		{"no payload", "POST", base + "/messages", `{"event_type":"a"}`, 422},
		{"null payload", "POST", base + "/messages", `{"event_type":"a","payload":null}`, 422},
		{"no event type", "POST", base + "/messages", `{"payload":{}}`, 422},
		{"send to missing app", "POST", "/v1/apps/app_missing/messages", `{"event_type":"a","payload":{}}`, 404},
		{"deliveries of missing message", "GET", base + "/messages/msg_x/deliveries", "", 404},
		{"attempts of missing delivery", "GET", base + "/deliveries/del_x/attempts", "", 404},
		{"bad limit", "GET", base + "/endpoints?limit=500", "", 400},
		{"max_in_flight too low", "POST", base + "/endpoints", `{"url":"https://x.io","max_in_flight":0}`, 422},
		{"max_in_flight too high", "POST", base + "/endpoints", `{"url":"https://x.io","max_in_flight":101}`, 422},
		{"negative rate_limit", "POST", base + "/endpoints", `{"url":"https://x.io","rate_limit":-1}`, 422},
		{"patch bad max_in_flight", "PATCH", base + "/endpoints/ep_x", `{"max_in_flight":0}`, 422},
		{"list endpoints of missing app", "GET", "/v1/apps/app_missing/endpoints", "", 404},
		{"replay missing delivery", "POST", base + "/deliveries/del_x/replay", "", 404},
		{"replay endpoint without since", "POST", base + "/endpoints/ep_x/replay", `{}`, 422},
		{"replay endpoint with bad since", "POST", base + "/endpoints/ep_x/replay", `{"since":"yesterday"}`, 400},
		{"replay missing endpoint", "POST", base + "/endpoints/ep_x/replay", `{"since":"2026-01-01T00:00:00Z"}`, 404},
		{"rotate with bad secret", "POST", base + "/endpoints/ep_x/secret/rotate", `{"secret":"hunter2"}`, 422},
		{"rotate missing endpoint", "POST", base + "/endpoints/ep_x/secret/rotate", "", 404},
		{"enable missing endpoint", "POST", base + "/endpoints/ep_x/enable", "", 404},
		{"replay endpoint with bad since param", "POST", base + "/endpoints/ep_x/replay?since=yesterday", "", 400},
		{"bad deliveries limit", "GET", base + "/messages/msg_x/deliveries?limit=0", "", 400},
		{"bad attempts cursor", "GET", base + "/deliveries/del_x/attempts?cursor=abc", "", 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := c.do(tt.method, tt.path, tt.body, nil)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d; body %s", rec.Code, tt.want, rec.Body.String())
			}
			if rec.Code >= 400 && rec.Header().Get("Content-Type") != "application/problem+json" {
				t.Errorf("error content type = %q", rec.Header().Get("Content-Type"))
			}
		})
	}

	// Another app's message is not visible.
	var other postgres.App
	c.expect(c.do("POST", "/v1/apps", `{"name":"other"}`, &other), http.StatusCreated)
	var sent struct{ ID string }
	c.expect(c.do("POST", "/v1/apps/"+other.ID+"/messages", `{"event_type":"a","payload":{}}`, &sent), http.StatusAccepted)
	c.expect(c.do("GET", base+"/messages/"+sent.ID+"/deliveries", "", nil), http.StatusNotFound)

	// A wrong key is rejected.
	req := httptest.NewRequest("GET", base, nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec := httptest.NewRecorder()
	c.h.ServeHTTP(rec, req)
	c.expect(rec, http.StatusUnauthorized)
}

func TestReplayAndRotate(t *testing.T) {
	c := newClient(t)
	var app postgres.App
	c.expect(c.do("POST", "/v1/apps", `{"name":"acme"}`, &app), http.StatusCreated)
	base := "/v1/apps/" + app.ID
	var ep postgres.Endpoint
	c.expect(c.do("POST", base+"/endpoints", `{"url":"https://example.com/hook"}`, &ep), http.StatusCreated)
	var sent struct{ ID string }
	c.expect(c.do("POST", base+"/messages", `{"event_type":"a","payload":{}}`, &sent), http.StatusAccepted)
	var ds list[postgres.Delivery]
	c.expect(c.do("GET", base+"/messages/"+sent.ID+"/deliveries", "", &ds), http.StatusOK)
	del := base + "/deliveries/" + ds.Data[0].ID + "/replay"

	// A pending delivery is still being worked on.
	c.expect(c.do("POST", del, "", nil), http.StatusConflict)

	if _, err := c.db.Exec(`UPDATE deliveries SET state = 'dead', attempt_count = 10`); err != nil {
		t.Fatal(err)
	}
	var d postgres.Delivery
	c.expect(c.do("POST", del, "", &d), http.StatusAccepted)
	if d.State != "pending" || d.AttemptCount != 0 || d.MessageID != sent.ID {
		t.Fatalf("replayed delivery = %+v", d)
	}

	if _, err := c.db.Exec(`UPDATE deliveries SET state = 'dead'`); err != nil {
		t.Fatal(err)
	}
	var replayed struct{ Replayed int }
	c.expect(c.do("POST", base+"/endpoints/"+ep.ID+"/replay?since=2000-01-01T00:00:00Z", "", &replayed), http.StatusAccepted)
	if replayed.Replayed != 1 {
		t.Fatalf("replayed = %d, want 1", replayed.Replayed)
	}
	// The JSON body form is accepted too.
	if _, err := c.db.Exec(`UPDATE deliveries SET state = 'dead'`); err != nil {
		t.Fatal(err)
	}
	c.expect(c.do("POST", base+"/endpoints/"+ep.ID+"/replay", `{"since":"2000-01-01T00:00:00Z"}`, &replayed), http.StatusAccepted)
	if replayed.Replayed != 1 {
		t.Fatalf("replayed with a body = %d, want 1", replayed.Replayed)
	}

	// enable re-enables an endpoint the service disabled.
	if _, err := c.db.Exec(`UPDATE endpoints SET status = 'disabled', disabled_at = now(), consecutive_failures = 50`); err != nil {
		t.Fatal(err)
	}
	var enabled postgres.Endpoint
	c.expect(c.do("POST", base+"/endpoints/"+ep.ID+"/enable", "", &enabled), http.StatusOK)
	if enabled.Status != "active" || enabled.DisabledAt != nil || enabled.ConsecutiveFailures != 0 || enabled.Secret != "" {
		t.Fatalf("enabled endpoint = %+v", enabled)
	}

	var rotated postgres.Endpoint
	c.expect(c.do("POST", base+"/endpoints/"+ep.ID+"/secret/rotate", "", &rotated), http.StatusOK)
	if !strings.HasPrefix(rotated.Secret, "whsec_") || rotated.Secret == ep.Secret {
		t.Fatalf("rotated secret = %q, want a new one", rotated.Secret)
	}
	const chosen = "whsec_bmV3c2VjcmV0bmV3c2VjcmV0"
	c.expect(c.do("POST", base+"/endpoints/"+ep.ID+"/secret/rotate", `{"secret":"`+chosen+`"}`, &rotated), http.StatusOK)
	if rotated.Secret != chosen {
		t.Fatalf("rotated secret = %q, want %q", rotated.Secret, chosen)
	}
}
