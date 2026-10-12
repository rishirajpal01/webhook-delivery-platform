// Command receiver is a minimal webhook receiver: it verifies each request
// with the Go SDK, skips duplicates, and answers 2xx once the event is
// handled.
//
//	WEBHOOK_SECRET=whsec_… go run ./examples/receiver
package main

import (
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/rishirajpal01/webhook-delivery-platform/webhooks"
)

func main() {
	secret := os.Getenv("WEBHOOK_SECRET") // the endpoint's secret, from its create response

	// Delivery is at least once, so remember what was handled. A real
	// receiver keeps this in its database, in the same transaction as the
	// work the event causes.
	var mu sync.Mutex
	seen := map[string]bool{}

	http.HandleFunc("POST /hooks", func(w http.ResponseWriter, r *http.Request) {
		event, err := webhooks.VerifyRequest(secret, r)
		if err != nil {
			http.Error(w, "invalid signature", http.StatusUnauthorized) // not from the service, or tampered
			return
		}
		id := r.Header.Get(webhooks.HeaderID)
		mu.Lock()
		defer mu.Unlock()
		if seen[id] {
			return // a retry of something already handled: 200, nothing to do
		}
		log.Printf("%s %s %s", id, event.Type, event.Data)
		seen[id] = true
	})
	log.Fatal(http.ListenAndServe(":9000", nil))
}
