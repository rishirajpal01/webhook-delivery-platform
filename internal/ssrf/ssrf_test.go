package ssrf

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"
)

func TestIsPublic(t *testing.T) {
	blocked := []string{
		"127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.169.254",
		"0.0.0.0", "100.64.0.1", "224.0.0.1", "255.255.255.255", "198.18.0.1",
		"::1", "::", "fe80::1", "fc00::1", "fd12::1", "ff02::1",
		"::ffff:10.0.0.1", "::ffff:127.0.0.1", "64:ff9b::a00:1", "2002:a00:1::",
	}
	for _, s := range blocked {
		if IsPublic(netip.MustParseAddr(s)) {
			t.Errorf("IsPublic(%s) = true, want false", s)
		}
	}
	allowed := []string{"8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111"}
	for _, s := range allowed {
		if !IsPublic(netip.MustParseAddr(s)) {
			t.Errorf("IsPublic(%s) = false, want true", s)
		}
	}
}

func TestDialerBlocksLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := NewDialer(time.Second, false).DialContext(ctx, "tcp", addr)
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("dial %s: got %v, want ErrBlocked", addr, err)
	}

	conn, err := NewDialer(time.Second, true).DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("allowPrivate dial: %v", err)
	}
	conn.Close()

	// A hostname is checked after it resolves.
	_, port, _ := net.SplitHostPort(addr)
	_, err = NewDialer(time.Second, false).DialContext(ctx, "tcp", net.JoinHostPort("localhost", port))
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("dial localhost: got %v, want ErrBlocked", err)
	}
}
