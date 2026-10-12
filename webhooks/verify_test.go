package webhooks

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/signing"
)

// signed builds a request the way the dispatcher does.
func signed(t *testing.T, secret, body string, at time.Time) *http.Request {
	t.Helper()
	sig, err := signing.Sign(secret, "msg_1", at, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/hooks", strings.NewReader(body))
	r.Header.Set(HeaderID, "msg_1")
	r.Header.Set(HeaderTimestamp, strconv.FormatInt(at.Unix(), 10))
	r.Header.Set(HeaderSignature, sig)
	return r
}

func TestVerifyRequest(t *testing.T) {
	secret, other := signing.NewSecret(), signing.NewSecret()
	body := `{"type":"order.paid","timestamp":"2026-10-12T07:00:00Z","data":{"order_id":"981"}}`

	e, err := VerifyRequest(secret, signed(t, secret, body, time.Now()))
	if err != nil || e.Type != "order.paid" || string(e.Data) != `{"order_id":"981"}` {
		t.Fatalf("VerifyRequest = %+v, %v", e, err)
	}

	tampered := signed(t, secret, body, time.Now())
	tampered.Body = httptest.NewRequest("POST", "/", strings.NewReader(strings.Replace(body, "981", "982", 1))).Body
	big := strings.Repeat(" ", MaxBodyBytes) + body
	tests := []struct {
		name string
		r    *http.Request
		want error
	}{
		{"wrong secret", signed(t, other, body, time.Now()), ErrNoMatch},
		{"tampered body", tampered, ErrNoMatch},
		{"too old", signed(t, secret, body, time.Now().Add(-10*time.Minute)), ErrBadTimestamp},
		{"too large", signed(t, secret, big, time.Now()), ErrBodyTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := VerifyRequest(secret, tt.r); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
	if _, err := VerifyRequest("not-a-secret", signed(t, secret, body, time.Now())); !errors.Is(err, ErrBadSecret) {
		t.Fatalf("bad secret: %v", err)
	}
}
