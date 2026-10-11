package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHandler(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tests := []struct {
		name   string
		opts   options
		random float64
		want   int
	}{
		{"never fails", options{failRate: 0, failStatus: 500}, 0.0, http.StatusOK},
		{"always fails", options{failRate: 1, failStatus: 503}, 0.99, http.StatusServiceUnavailable},
		{"roll above rate succeeds", options{failRate: 0.3, failStatus: 500}, 0.5, http.StatusOK},
		{"roll below rate fails", options{failRate: 0.3, failStatus: 500}, 0.1, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHandler(tt.opts, func() float64 { return tt.random }, logger)
			req := httptest.NewRequest("POST", "/hooks", strings.NewReader(`{"a":1}`))
			req.Header.Set("webhook-id", "msg_test")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Errorf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestHandlerLatency(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := newHandler(options{latency: 50 * time.Millisecond}, func() float64 { return 1 }, logger)
	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", nil))
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("responded after %v, want at least 50ms", elapsed)
	}
}
