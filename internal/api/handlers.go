package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/signing"
)

const maxNameLen = 255

var messagesCreated = promauto.NewCounter(prometheus.CounterOpts{
	Name: "webhookd_messages_created_total",
	Help: "Messages accepted by the API, not counting idempotent repeats.",
})

// Request bodies, named so the contract test can check them against
// docs/openapi.json.
type (
	createAppRequest struct {
		Name string `json:"name"`
	}
	createEndpointRequest struct {
		URL         string   `json:"url"`
		EventTypes  []string `json:"event_types"`
		Secret      string   `json:"secret"`
		MaxInFlight *int     `json:"max_in_flight"`
		RateLimit   *int     `json:"rate_limit"`
	}
	updateEndpointRequest struct {
		URL         *string   `json:"url"`
		EventTypes  *[]string `json:"event_types"`
		Status      *string   `json:"status"`
		MaxInFlight *int      `json:"max_in_flight"`
		RateLimit   *int      `json:"rate_limit"`
	}
	rotateSecretRequest struct {
		Secret string `json:"secret"`
	}
	replayEndpointRequest struct {
		Since *time.Time `json:"since"`
	}
	sendMessageRequest struct {
		EventType string          `json:"event_type"`
		Payload   json.RawMessage `json:"payload"`
	}
	// sendMessageResponse is what a send returns.
	sendMessageResponse struct {
		ID         string `json:"id"`
		Deliveries int    `json:"deliveries"`
	}
	replayEndpointResponse struct {
		Replayed int `json:"replayed"`
	}
)

// --- applications ---

func (s *server) createApp(w http.ResponseWriter, r *http.Request) {
	var req createAppRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Name == "" || len(req.Name) > maxNameLen {
		problem(w, http.StatusUnprocessableEntity, "name is required and must be at most 255 bytes")
		return
	}
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	app, dup, err := s.store.CreateAppIdempotent(r.Context(), req.Name, key)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeCreated(w, dup, app)
}

func (s *server) getApp(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, app)
}

// --- endpoints ---

func (s *server) createEndpoint(w http.ResponseWriter, r *http.Request) {
	var req createEndpointRequest
	if !decode(w, r, &req) {
		return
	}
	if msg := validateURL(req.URL); msg != "" {
		problem(w, http.StatusUnprocessableEntity, msg)
		return
	}
	if msg := validateEventTypes(req.EventTypes); msg != "" {
		problem(w, http.StatusUnprocessableEntity, msg)
		return
	}
	if msg := validateLimits(req.MaxInFlight, req.RateLimit); msg != "" {
		problem(w, http.StatusUnprocessableEntity, msg)
		return
	}
	if req.Secret == "" {
		req.Secret = signing.NewSecret()
	} else if !signing.ValidSecret(req.Secret) {
		problem(w, http.StatusUnprocessableEntity, "secret must be whsec_ followed by base64 of at least 16 bytes")
		return
	}

	ep := postgres.Endpoint{AppID: r.PathValue("app"), URL: req.URL, EventTypes: req.EventTypes, Secret: req.Secret}
	if req.MaxInFlight != nil {
		ep.MaxInFlight = *req.MaxInFlight
	}
	if req.RateLimit != nil {
		ep.RateLimit = *req.RateLimit
	}
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	ep, dup, err := s.store.CreateEndpointIdempotent(r.Context(), ep, key)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// The secret is returned here, on create (and on an idempotent repeat of
	// it), so the operator can hand it to the receiver.
	writeCreated(w, dup, ep)
}

func (s *server) listEndpoints(w http.ResponseWriter, r *http.Request) {
	limit, ok := pageLimit(w, r)
	if !ok {
		return
	}
	appID := r.PathValue("app")
	if _, err := s.store.GetApp(r.Context(), appID); err != nil {
		s.fail(w, r, err)
		return
	}
	eps, err := s.store.ListEndpoints(r.Context(), appID, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := list[postgres.Endpoint]{Data: eps}
	for i := range out.Data {
		out.Data[i].Secret = ""
	}
	if len(eps) == limit {
		out.NextCursor = eps[len(eps)-1].ID
	}
	writeJSON(w, http.StatusOK, out)
}

// enableEndpoint re-enables an endpoint, typically one the service disabled
// after it kept failing. Its failure history is cleared, so it starts again
// with a closed circuit. It is the same as PATCH {"status": "active"}.
func (s *server) enableEndpoint(w http.ResponseWriter, r *http.Request) {
	active := postgres.EndpointActive
	ep, err := s.store.UpdateEndpoint(r.Context(), r.PathValue("app"), r.PathValue("ep"),
		postgres.EndpointPatch{Status: &active})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ep.Secret = ""
	writeJSON(w, http.StatusOK, ep)
}

func (s *server) getEndpoint(w http.ResponseWriter, r *http.Request) {
	ep, err := s.store.GetEndpoint(r.Context(), r.PathValue("app"), r.PathValue("ep"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ep.Secret = ""
	writeJSON(w, http.StatusOK, ep)
}

func (s *server) updateEndpoint(w http.ResponseWriter, r *http.Request) {
	var req updateEndpointRequest
	if !decode(w, r, &req) {
		return
	}
	if req.URL != nil {
		if msg := validateURL(*req.URL); msg != "" {
			problem(w, http.StatusUnprocessableEntity, msg)
			return
		}
	}
	if req.EventTypes != nil {
		if msg := validateEventTypes(*req.EventTypes); msg != "" {
			problem(w, http.StatusUnprocessableEntity, msg)
			return
		}
	}
	if msg := validateLimits(req.MaxInFlight, req.RateLimit); msg != "" {
		problem(w, http.StatusUnprocessableEntity, msg)
		return
	}
	// "disabled" is set by the service when an endpoint keeps failing.
	if req.Status != nil && *req.Status != postgres.EndpointActive && *req.Status != postgres.EndpointPaused {
		problem(w, http.StatusUnprocessableEntity, `status must be "active" or "paused"`)
		return
	}

	ep, err := s.store.UpdateEndpoint(r.Context(), r.PathValue("app"), r.PathValue("ep"), postgres.EndpointPatch{
		URL: req.URL, EventTypes: req.EventTypes, Status: req.Status,
		MaxInFlight: req.MaxInFlight, RateLimit: req.RateLimit,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	ep.Secret = ""
	writeJSON(w, http.StatusOK, ep)
}

func (s *server) deleteEndpoint(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteEndpoint(r.Context(), r.PathValue("app"), r.PathValue("ep")); err != nil {
		s.fail(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// rotationOverlap is how long the old secret keeps signing requests after
// a rotation, so receivers have a day to switch over.
const rotationOverlap = 24 * time.Hour

func (s *server) rotateSecret(w http.ResponseWriter, r *http.Request) {
	var req rotateSecretRequest
	if r.ContentLength != 0 && !decode(w, r, &req) { // the body is optional
		return
	}
	if req.Secret == "" {
		req.Secret = signing.NewSecret()
	} else if !signing.ValidSecret(req.Secret) {
		problem(w, http.StatusUnprocessableEntity, "secret must be whsec_ followed by base64 of at least 16 bytes")
		return
	}
	ep, err := s.store.RotateSecret(r.Context(), r.PathValue("app"), r.PathValue("ep"), req.Secret, rotationOverlap)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	// Like on create, the new secret is returned once, here.
	writeJSON(w, http.StatusOK, ep)
}

// replayEndpoint redelivers every dead delivery to the endpoint created at
// or after ?since=<RFC 3339 time>, e.g. after the receiver's outage is fixed.
// A JSON body {"since": ...} is accepted too.
func (s *server) replayEndpoint(w http.ResponseWriter, r *http.Request) {
	var req replayEndpointRequest
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			problem(w, http.StatusBadRequest, "since must be an RFC 3339 time such as 2026-10-11T00:00:00Z")
			return
		}
		req.Since = &t
	} else if r.ContentLength != 0 && !decode(w, r, &req) {
		return
	}
	if req.Since == nil {
		problem(w, http.StatusUnprocessableEntity, "since is required (RFC 3339 time)")
		return
	}
	n, err := s.store.ReplayEndpoint(r.Context(), r.PathValue("app"), r.PathValue("ep"), *req.Since)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, replayEndpointResponse{Replayed: n})
}

// --- messages ---

func (s *server) sendMessage(w http.ResponseWriter, r *http.Request) {
	var req sendMessageRequest
	if !decode(w, r, &req) {
		return
	}
	if req.EventType == "" || len(req.EventType) > maxNameLen {
		problem(w, http.StatusUnprocessableEntity, "event_type is required and must be at most 255 bytes")
		return
	}
	if len(req.Payload) == 0 || bytes.Equal(req.Payload, []byte("null")) {
		problem(w, http.StatusUnprocessableEntity, "payload is required")
		return
	}
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}

	res, err := s.store.CreateMessage(r.Context(), postgres.Message{
		AppID: r.PathValue("app"), EventType: req.EventType, Payload: req.Payload, IdempotencyKey: key,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}

	status := http.StatusAccepted
	if res.Duplicate {
		// Same answer as the first send; nothing new was queued.
		w.Header().Set("Idempotent-Replayed", "true")
		status = http.StatusOK
	} else {
		messagesCreated.Inc()
	}
	writeJSON(w, status, sendMessageResponse{ID: res.Message.ID, Deliveries: res.Deliveries})
}

func (s *server) listDeliveries(w http.ResponseWriter, r *http.Request) {
	limit, ok := pageLimit(w, r)
	if !ok {
		return
	}
	ds, err := s.store.ListDeliveries(r.Context(), r.PathValue("app"), r.PathValue("msg"),
		r.URL.Query().Get("cursor"), limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := list[postgres.Delivery]{Data: ds}
	if len(ds) == limit {
		out.NextCursor = ds[len(ds)-1].ID
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *server) listAttempts(w http.ResponseWriter, r *http.Request) {
	limit, ok := pageLimit(w, r)
	if !ok {
		return
	}
	var after int64
	if v := r.URL.Query().Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			problem(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		after = n
	}
	as, err := s.store.ListAttempts(r.Context(), r.PathValue("app"), r.PathValue("del"), after, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := list[postgres.Attempt]{Data: as}
	if len(as) == limit {
		out.NextCursor = strconv.FormatInt(as[len(as)-1].ID, 10)
	}
	writeJSON(w, http.StatusOK, out)
}

// replayDelivery sends a dead or succeeded delivery again, with the same
// webhook-id, starting a fresh retry schedule.
func (s *server) replayDelivery(w http.ResponseWriter, r *http.Request) {
	d, err := s.store.ReplayDelivery(r.Context(), r.PathValue("app"), r.PathValue("del"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, d)
}

// --- request helpers ---

// pageLimit reads ?limit= (default 50, at most 100). It writes the problem
// response itself and returns false when the value is invalid.
func pageLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return 50, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 100 {
		problem(w, http.StatusBadRequest, "limit must be between 1 and 100")
		return 0, false
	}
	return n, true
}

// idempotencyKey reads the optional Idempotency-Key header. It writes the
// problem response itself and returns false when the key is too long.
func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxNameLen {
		problem(w, http.StatusBadRequest, "Idempotency-Key must be at most 255 bytes")
		return "", false
	}
	return key, true
}

// writeCreated answers a create: 201 with the new object, or, when an
// Idempotency-Key matched an earlier create, 200 with that object and an
// Idempotent-Replayed header.
func writeCreated(w http.ResponseWriter, duplicate bool, v any) {
	if duplicate {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, v)
		return
	}
	writeJSON(w, http.StatusCreated, v)
}

// --- validation ---

// validateURL returns a problem detail, or "" if u is acceptable. Whether
// the host is public is checked at send time, after DNS resolution.
func validateURL(u string) string {
	if len(u) > 2048 {
		return "url must be at most 2048 bytes"
	}
	parsed, err := url.Parse(u)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "url must be an absolute http or https URL"
	}
	if parsed.User != nil {
		return "url must not contain credentials"
	}
	return ""
}

func validateEventTypes(types []string) string {
	if len(types) > 100 {
		return "event_types may list at most 100 types"
	}
	for _, t := range types {
		if t == "" || len(t) > maxNameLen {
			return fmt.Sprintf("event type %q must be 1 to 255 bytes", t)
		}
	}
	return ""
}

// validateLimits checks an endpoint's concurrency cap and rate limit (nil
// means not given).
func validateLimits(maxInFlight, rateLimit *int) string {
	if maxInFlight != nil && (*maxInFlight < 1 || *maxInFlight > 100) {
		return "max_in_flight must be between 1 and 100"
	}
	if rateLimit != nil && (*rateLimit < 0 || *rateLimit > 10000) {
		return "rate_limit must be between 0 (unlimited) and 10000 requests per second"
	}
	return ""
}
