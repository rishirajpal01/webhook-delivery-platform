// Package ssrf stops webhook deliveries from reaching internal networks.
//
// The check runs in net.Dialer.Control, after DNS resolution and right
// before connecting, so a hostname that resolves to a private address (or
// re-resolves to one later, as in DNS rebinding) is still refused.
package ssrf

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"syscall"
	"time"
)

// ErrBlocked is returned when a destination address is not public.
var ErrBlocked = errors.New("ssrf: destination address is not allowed")

// Ranges that IsPublic rejects beyond what netip's helpers cover.
var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),     // "this network"
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),  // IETF protocol assignments
	netip.MustParsePrefix("198.18.0.0/15"), // benchmarking
	netip.MustParsePrefix("240.0.0.0/4"),   // reserved, includes broadcast
	netip.MustParsePrefix("64:ff9b::/96"),  // NAT64, can embed private IPv4
	netip.MustParsePrefix("2002::/16"),     // 6to4, can embed private IPv4
	netip.MustParsePrefix("::/96"),         // IPv4-compatible (deprecated), embeds IPv4
	netip.MustParsePrefix("2001::/32"),     // Teredo, embeds IPv4
	netip.MustParsePrefix("2001:db8::/32"), // documentation
}

// IsPublic reports whether addr is a globally routable unicast address.
// Loopback, private, link-local (including cloud metadata at
// 169.254.169.254), multicast and reserved ranges are not.
func IsPublic(addr netip.Addr) bool {
	addr = addr.Unmap() // treat ::ffff:10.0.0.1 as 10.0.0.1
	if !addr.IsValid() || !addr.IsGlobalUnicast() || addr.IsPrivate() {
		return false
	}
	for _, p := range blockedPrefixes {
		if p.Contains(addr) {
			return false
		}
	}
	return true
}

// NewDialer returns a dialer that refuses non-public addresses. With
// allowPrivate set it allows everything, for local development and tests.
func NewDialer(timeout time.Duration, allowPrivate bool) *net.Dialer {
	d := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	if !allowPrivate {
		d.Control = control
	}
	return d
}

func control(network, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return err
	}
	if !IsPublic(addr) {
		return fmt.Errorf("%w: %s", ErrBlocked, addr)
	}
	return nil
}
