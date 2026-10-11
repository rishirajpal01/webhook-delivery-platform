// Command mockreceiver is a webhook receiver for demos and tests. It accepts
// any POST, logs the webhook headers, and can be told to be slow or flaky so
// retries are easy to see.
//
//	mockreceiver -addr :9000 -fail-rate 0.3 -latency 200ms -secret whsec_...
//
// With -secret it verifies each request's Standard Webhooks signature and
// answers 401 when it does not match.
package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/signing"
)

type options struct {
	failRate   float64       // share of requests answered with failStatus, 0 to 1
	failStatus int           // status code for a failed request
	latency    time.Duration // delay before every response
	secret     string        // if set, signatures are verified with it
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	var opts options
	flag.Float64Var(&opts.failRate, "fail-rate", 0, "share of requests to fail, 0 to 1")
	flag.IntVar(&opts.failStatus, "fail-status", http.StatusInternalServerError, "status code for failed requests")
	flag.DurationVar(&opts.latency, "latency", 0, "delay before every response")
	flag.StringVar(&opts.secret, "secret", "", "endpoint secret (whsec_...) to verify signatures with")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if opts.failRate < 0 || opts.failRate > 1 {
		logger.Error("fail-rate must be between 0 and 1")
		os.Exit(2)
	}
	if opts.secret != "" && !signing.ValidSecret(opts.secret) {
		logger.Error("secret must be whsec_ followed by base64")
		os.Exit(2)
	}

	srv := &http.Server{
		Addr:              *addr,
		Handler:           newHandler(opts, rand.Float64, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}
	logger.Info("mock receiver listening", "addr", *addr, "fail_rate", opts.failRate, "latency", opts.latency.String(), "verify_signatures", opts.secret != "")
	if err := srv.ListenAndServe(); err != nil {
		logger.Error("mock receiver exited", "err", err)
		os.Exit(1)
	}
}

// newHandler takes the random source as a function so tests can make the
// fail decision deterministic.
func newHandler(opts options, random func() float64, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if opts.latency > 0 {
			select {
			case <-time.After(opts.latency):
			case <-r.Context().Done():
				return
			}
		}

		status := http.StatusOK
		verified := ""
		if opts.secret != "" {
			err := signing.Verify(opts.secret, r.Header.Get(signing.HeaderID), r.Header.Get(signing.HeaderTimestamp),
				r.Header.Get(signing.HeaderSignature), body, time.Now(), 5*time.Minute)
			verified = "ok"
			if err != nil {
				verified = err.Error()
				status = http.StatusUnauthorized
			}
		}
		if status == http.StatusOK && random() < opts.failRate {
			status = opts.failStatus
		}
		logger.Info("webhook received",
			"path", r.URL.Path,
			"webhook_id", r.Header.Get(signing.HeaderID),
			"webhook_timestamp", r.Header.Get(signing.HeaderTimestamp),
			"signature", verified,
			"body_bytes", len(body),
			"status", status,
		)
		w.WriteHeader(status)
	})
	return mux
}
