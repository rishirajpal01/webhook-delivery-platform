// Package signing implements Standard Webhooks request signing
// (https://www.standardwebhooks.com/).
//
// A request carries three headers:
//
//	webhook-id:        the message id, stable across retries
//	webhook-timestamp: Unix seconds when this attempt was sent
//	webhook-signature: "v1,<base64 HMAC-SHA256 of id.timestamp.body>"
//
// The signature header may hold several space-separated signatures (during
// secret rotation); a receiver accepts the request if any of them matches.
package signing

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

const secretPrefix = "whsec_"

// Header names defined by the spec.
const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
)

// Errors returned by Verify.
var (
	ErrBadSecret    = errors.New("signing: secret must be whsec_ followed by base64")
	ErrBadTimestamp = errors.New("signing: timestamp missing, malformed or outside tolerance")
	ErrNoMatch      = errors.New("signing: no signature matched")
)

// NewSecret returns a fresh secret in the whsec_<base64> format, carrying 24
// random bytes.
func NewSecret() string {
	b := make([]byte, 24)
	rand.Read(b)
	return secretPrefix + base64.StdEncoding.EncodeToString(b)
}

// ValidSecret reports whether s is a usable secret.
func ValidSecret(s string) bool {
	k, err := key(s)
	return err == nil && len(k) >= 16
}

// Sign returns the webhook-signature header value for one request.
func Sign(secret, msgID string, ts time.Time, body []byte) (string, error) {
	k, err := key(secret)
	if err != nil {
		return "", err
	}
	return "v1," + mac(k, msgID, ts.Unix(), body), nil
}

// Verify checks a request's headers against secret. tolerance bounds how far
// the timestamp may be from now, which limits replay of captured requests.
func Verify(secret, msgID, timestamp, signatures string, body []byte, now time.Time, tolerance time.Duration) error {
	k, err := key(secret)
	if err != nil {
		return err
	}
	ts, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	if d := now.Sub(time.Unix(ts, 0)); d > tolerance || d < -tolerance {
		return ErrBadTimestamp
	}

	want := mac(k, msgID, ts, body)
	for sig := range strings.FieldsSeq(signatures) {
		version, value, ok := strings.Cut(sig, ",")
		if ok && version == "v1" && hmac.Equal([]byte(value), []byte(want)) {
			return nil
		}
	}
	return ErrNoMatch
}

func key(secret string) ([]byte, error) {
	raw, ok := strings.CutPrefix(secret, secretPrefix)
	if !ok {
		return nil, ErrBadSecret
	}
	k, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, ErrBadSecret
	}
	return k, nil
}

func mac(key []byte, msgID string, ts int64, body []byte) string {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msgID + "." + strconv.FormatInt(ts, 10) + "."))
	h.Write(body)
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
