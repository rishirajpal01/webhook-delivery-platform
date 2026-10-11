// Package api serves the HTTP API.
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
)

// Pinger reports whether the database is reachable. *sql.DB satisfies it.
type Pinger interface {
	PingContext(ctx context.Context) error
}

type server struct {
	store   *postgres.Store
	keyHash [32]byte
	logger  *slog.Logger
}

// NewOpsHandler serves only the operational routes, for processes that
// do not serve the API (dispatchers):
//
//	GET /healthz  liveness: the process is up and serving
//	GET /readyz   readiness: the process can reach Postgres
//	GET /metrics  Prometheus metrics
func NewOpsHandler(db Pinger, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	addOpsRoutes(mux, db, logger)
	return mux
}

// NewHandler serves the operational routes plus the API. Everything under
// /v1 requires "Authorization: Bearer <apiKey>".
func NewHandler(db Pinger, store *postgres.Store, apiKey string, logger *slog.Logger) http.Handler {
	s := &server{store: store, keyHash: sha256.Sum256([]byte(apiKey)), logger: logger}
	mux := http.NewServeMux()
	addOpsRoutes(mux, db, logger)

	v1 := http.NewServeMux()
	v1.HandleFunc("POST /v1/apps", s.createApp)
	v1.HandleFunc("GET /v1/apps/{app}", s.getApp)
	v1.HandleFunc("POST /v1/apps/{app}/endpoints", s.createEndpoint)
	v1.HandleFunc("GET /v1/apps/{app}/endpoints", s.listEndpoints)
	v1.HandleFunc("GET /v1/apps/{app}/endpoints/{ep}", s.getEndpoint)
	v1.HandleFunc("PATCH /v1/apps/{app}/endpoints/{ep}", s.updateEndpoint)
	v1.HandleFunc("DELETE /v1/apps/{app}/endpoints/{ep}", s.deleteEndpoint)
	v1.HandleFunc("POST /v1/apps/{app}/messages", s.sendMessage)
	v1.HandleFunc("GET /v1/apps/{app}/messages/{msg}/deliveries", s.listDeliveries)
	v1.HandleFunc("GET /v1/apps/{app}/deliveries/{del}/attempts", s.listAttempts)
	mux.Handle("/v1/", s.requireKey(v1))

	return mux
}

func addOpsRoutes(mux *http.ServeMux, db Pinger, logger *slog.Logger) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			logger.Warn("readiness check failed", "err", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", promhttp.Handler())
}

// requireKey checks the bearer token. Both sides are hashed first so the
// comparison is constant-time whatever the token's length.
func (s *server) requireKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		got := sha256.Sum256([]byte(token))
		if !ok || subtle.ConstantTimeCompare(got[:], s.keyHash[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			problem(w, http.StatusUnauthorized, "missing or invalid API key")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// fail writes the right problem for err, logging anything unexpected.
func (s *server) fail(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, postgres.ErrNotFound) {
		problem(w, http.StatusNotFound, "not found")
		return
	}
	s.logger.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	problem(w, http.StatusInternalServerError, "internal error")
}

// problem writes an RFC 9457 problem details response.
func problem(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{
		"type":   "about:blank",
		"title":  http.StatusText(status),
		"status": status,
		"detail": detail,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// decode reads a JSON body of at most 1 MiB into v, rejecting unknown
// fields. It writes the problem response itself and returns false on error.
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			problem(w, http.StatusRequestEntityTooLarge, "request body is larger than 1 MiB")
		} else {
			problem(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		}
		return false
	}
	return true
}

type list[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"next_cursor,omitempty"`
}
