package webhooks

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/signing"
)

// Standard Webhooks headers on every request.
const (
	HeaderID        = signing.HeaderID        // the message id: deduplicate on it
	HeaderTimestamp = signing.HeaderTimestamp // Unix seconds when this attempt was sent
	HeaderSignature = signing.HeaderSignature // one or more "v1,<base64 HMAC-SHA256>"
)

// Tolerance is how far a request's timestamp may be from now. Older requests
// are rejected, so a captured request cannot be replayed later.
const Tolerance = 5 * time.Minute

// MaxBodyBytes bounds the body VerifyRequest reads. The API accepts payloads
// up to 1 MiB, and the envelope adds a little.
const MaxBodyBytes = 2 << 20

// Errors from Verify and VerifyRequest.
var (
	ErrBadSecret    = signing.ErrBadSecret    // the secret is not whsec_ followed by base64
	ErrBadTimestamp = signing.ErrBadTimestamp // missing, malformed or outside Tolerance
	ErrNoMatch      = signing.ErrNoMatch      // no signature matched: not from us, or tampered
	ErrBodyTooLarge = errors.New("webhooks: request body is larger than MaxBodyBytes")
)

// Verify checks that a request with these headers and this exact body was
// signed with secret (the endpoint's whsec_ secret) within Tolerance. During
// a secret rotation a request carries several signatures; one match is
// enough.
func Verify(secret string, header http.Header, body []byte) error {
	return signing.Verify(secret, header.Get(HeaderID), header.Get(HeaderTimestamp),
		header.Get(HeaderSignature), body, time.Now(), Tolerance)
}

// VerifyRequest reads r's body, verifies its signature with Verify and
// decodes the event. Answer 2xx only after the event is safely handled, and
// skip events whose webhook-id (r.Header.Get(HeaderID)) you have already
// processed: delivery is at least once.
func VerifyRequest(secret string, r *http.Request) (*Event, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > MaxBodyBytes {
		return nil, ErrBodyTooLarge
	}
	if err := Verify(secret, r.Header, body); err != nil {
		return nil, err
	}
	var e Event
	if err := json.Unmarshal(body, &e); err != nil {
		return nil, err
	}
	return &e, nil
}
