package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthcheck(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(status)
	}))
	addr := strings.TrimPrefix(srv.URL, "http://")
	if got := healthcheck(addr); got != 0 {
		t.Errorf("ready: exit %d, want 0", got)
	}
	status = http.StatusServiceUnavailable
	if got := healthcheck(addr); got != 1 {
		t.Errorf("not ready: exit %d, want 1", got)
	}
	srv.Close()
	if got := healthcheck(addr); got != 1 {
		t.Errorf("not listening: exit %d, want 1", got)
	}
}
