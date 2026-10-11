package signing

import (
	"errors"
	"strconv"
	"testing"
	"time"
)

// Test vector from the Standard Webhooks specification.
const (
	specSecret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	specID     = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	specTS     = 1614265330
	specBody   = `{"test": 2432232314}`
	specSig    = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="
)

func TestSignMatchesSpecVector(t *testing.T) {
	got, err := Sign(specSecret, specID, time.Unix(specTS, 0), []byte(specBody))
	if err != nil {
		t.Fatal(err)
	}
	if got != specSig {
		t.Errorf("Sign = %q, want %q", got, specSig)
	}
}

func TestVerify(t *testing.T) {
	now := time.Unix(specTS, 0)
	ts := strconv.Itoa(specTS)
	tol := 5 * time.Minute
	other := NewSecret()
	otherSig, _ := Sign(other, specID, now, []byte(specBody))

	tests := []struct {
		name    string
		secret  string
		ts      string
		sigs    string
		body    string
		now     time.Time
		wantErr error
	}{
		{"valid", specSecret, ts, specSig, specBody, now, nil},
		{"valid among several (rotation)", specSecret, ts, otherSig + " " + specSig, specBody, now, nil},
		{"tampered body", specSecret, ts, specSig, `{"test": 1}`, now, ErrNoMatch},
		{"wrong secret", other, ts, specSig, specBody, now, ErrNoMatch},
		{"unknown version", specSecret, ts, "v2," + specSig[3:], specBody, now, ErrNoMatch},
		{"timestamp too old", specSecret, ts, specSig, specBody, now.Add(10 * time.Minute), ErrBadTimestamp},
		{"timestamp in future", specSecret, ts, specSig, specBody, now.Add(-10 * time.Minute), ErrBadTimestamp},
		{"timestamp garbage", specSecret, "soon", specSig, specBody, now, ErrBadTimestamp},
		{"bad secret", "nope", ts, specSig, specBody, now, ErrBadSecret},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Verify(tt.secret, specID, tt.ts, tt.sigs, []byte(tt.body), tt.now, tol)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Verify = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewSecretRoundTrips(t *testing.T) {
	s := NewSecret()
	if !ValidSecret(s) {
		t.Fatalf("NewSecret produced invalid secret %q", s)
	}
	now := time.Now()
	sig, err := Sign(s, "msg_1", now, []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if err := Verify(s, "msg_1", strconv.FormatInt(now.Unix(), 10), sig, []byte("{}"), now, time.Minute); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "whsec_", "whsec_!!!", "abc_" + s[6:], "whsec_c2hvcnQ="} {
		if ValidSecret(bad) {
			t.Errorf("ValidSecret(%q) = true", bad)
		}
	}
}
