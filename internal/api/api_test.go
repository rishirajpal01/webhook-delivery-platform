package api

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakePinger struct{ err error }

func (f fakePinger) PingContext(context.Context) error { return f.err }

func TestRoutes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
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
		{"wrong method", "POST", "/healthz", nil, http.StatusMethodNotAllowed},
		{"unknown path", "GET", "/nope", nil, http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(fakePinger{tt.dbErr}, logger)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}
