// Command loadgen measures delivery throughput and lag end to end.
//
// It runs its own webhook receiver, creates an app with -endpoints endpoints
// pointing at it, sends -messages events through the API (at -rate per
// second, or as fast as -senders workers can), and waits for every delivery
// to arrive. Lag is the time from the send call starting to the delivery
// first reaching the receiver.
//
//	loadgen -api http://localhost:8080 -messages 20000 -endpoints 20 -latency 20ms
//
// The dispatchers must be able to reach -receiver-url and must run with
// ALLOW_PRIVATE_NETWORKS=true when it is a local address.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type options struct {
	api, key    string
	listen      string
	receiverURL string
	messages    int
	endpoints   int
	maxInFlight int
	senders     int
	rate        float64
	latency     time.Duration
	timeout     time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.api, "api", "http://localhost:8080", "webhookd API base URL")
	flag.StringVar(&o.key, "key", envOr("API_KEY", "dev-api-key-change-me"), "API key")
	flag.StringVar(&o.listen, "listen", "127.0.0.1:9100", "receiver listen address")
	flag.StringVar(&o.receiverURL, "receiver-url", "", "receiver URL as the dispatchers see it (default http://<listen>)")
	flag.IntVar(&o.messages, "messages", 10000, "messages to send")
	flag.IntVar(&o.endpoints, "endpoints", 20, "endpoints each message fans out to")
	flag.IntVar(&o.maxInFlight, "max-in-flight", 4, "concurrency cap per endpoint")
	flag.IntVar(&o.senders, "senders", 16, "parallel send calls")
	flag.Float64Var(&o.rate, "rate", 0, "messages per second to send; 0 sends as fast as possible")
	flag.DurationVar(&o.latency, "latency", 0, "how long the receiver takes to answer")
	flag.DurationVar(&o.timeout, "timeout", 10*time.Minute, "give up if deliveries have not all arrived by then")
	flag.Parse()
	if o.receiverURL == "" {
		o.receiverURL = "http://" + o.listen
	}
	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func run(o options) error {
	c := client{base: o.api, key: o.key, http: &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{MaxIdleConnsPerHost: o.senders},
	}}
	var app struct{ ID string }
	if err := c.post("/v1/apps", map[string]any{"name": "loadgen"}, &app); err != nil {
		return err
	}
	// Endpoint paths carry the app id, so retries left over from an earlier
	// run against the same receiver address are not counted.
	prefix := "/ep/" + app.ID + "/"
	rec := newReceiver(prefix, o.latency)
	ln, err := net.Listen("tcp", o.listen)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: rec, ReadHeaderTimeout: 5 * time.Second}
	go srv.Serve(ln)
	// Let requests in progress finish, so the dispatchers do not see an
	// error and retry them into the next run.
	defer srv.Shutdown(context.Background())
	for i := range o.endpoints {
		ep := map[string]any{"url": fmt.Sprintf("%s%s%d", o.receiverURL, prefix, i), "max_in_flight": o.maxInFlight}
		if err := c.post("/v1/apps/"+app.ID+"/endpoints", ep, nil); err != nil {
			return err
		}
	}
	fmt.Printf("app %s: %d messages x %d endpoints, max_in_flight %d, receiver latency %v\n",
		app.ID, o.messages, o.endpoints, o.maxInFlight, o.latency)

	// Senders take message numbers from next; with -rate a ticker paces them.
	var (
		mu      sync.Mutex
		sentAt  = make(map[string]time.Time, o.messages)
		next    atomic.Int64
		sendErr atomic.Value
		wg      sync.WaitGroup
		pace    <-chan time.Time
		start   = time.Now()
		body, _ = json.Marshal(map[string]any{"event_type": "load.test", "payload": map[string]string{"pad": strings.Repeat("x", 200)}})
		path    = "/v1/apps/" + app.ID + "/messages"
	)
	if o.rate > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / o.rate))
		defer t.Stop()
		pace = t.C
	}
	for range o.senders {
		wg.Go(func() {
			for next.Add(1) <= int64(o.messages) && sendErr.Load() == nil {
				if pace != nil {
					<-pace
				}
				at := time.Now()
				var msg struct{ ID string }
				if err := c.postRaw(path, body, &msg); err != nil {
					sendErr.Store(err)
					return
				}
				mu.Lock()
				sentAt[msg.ID] = at
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if err, _ := sendErr.Load().(error); err != nil {
		return err
	}
	sendTime := time.Since(start)
	fmt.Printf("sent %d messages in %v (%.0f/s)\n", o.messages, sendTime.Round(time.Millisecond), float64(o.messages)/sendTime.Seconds())

	want := o.messages * o.endpoints
	deadline := time.Now().Add(o.timeout)
	for rec.count() < want {
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d of %d deliveries arrived within %v", rec.count(), want, o.timeout)
		}
		time.Sleep(50 * time.Millisecond)
	}

	first, dupes := rec.results()
	var (
		lags []time.Duration
		last time.Time
	)
	for k, at := range first {
		msgID, _, _ := strings.Cut(k, "|")
		lags = append(lags, at.Sub(sentAt[msgID]))
		if at.After(last) {
			last = at
		}
	}
	slices.Sort(lags)
	elapsed := last.Sub(start)
	pct := func(p float64) time.Duration { return lags[int(float64(len(lags)-1)*p)].Round(time.Millisecond) }
	fmt.Printf("delivered %d in %v: %.0f deliveries/s; lag p50 %v, p99 %v, max %v; %d duplicates\n",
		len(lags), elapsed.Round(time.Millisecond), float64(len(lags))/elapsed.Seconds(),
		pct(0.5), pct(0.99), pct(1), dupes)
	return nil
}

// receiver records when each (message, endpoint) pair first arrives.
type receiver struct {
	prefix  string
	latency time.Duration
	mu      sync.Mutex
	first   map[string]time.Time
	dupes   int
}

func newReceiver(prefix string, latency time.Duration) *receiver {
	return &receiver{prefix: prefix, latency: latency, first: map[string]time.Time{}}
}

func (r *receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	at := time.Now()
	if !strings.HasPrefix(req.URL.Path, r.prefix) {
		return // from an earlier run
	}
	key := req.Header.Get("webhook-id") + "|" + req.URL.Path
	r.mu.Lock()
	if _, seen := r.first[key]; seen {
		r.dupes++
	} else {
		r.first[key] = at
	}
	r.mu.Unlock()
	if r.latency > 0 {
		time.Sleep(r.latency)
	}
}

func (r *receiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.first)
}

func (r *receiver) results() (map[string]time.Time, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.first, r.dupes
}

type client struct {
	base, key string
	http      *http.Client
}

func (c client) post(path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return c.postRaw(path, body, out)
}

func (c client) postRaw(path string, body []byte, out any) error {
	req, err := http.NewRequest(http.MethodPost, c.base+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var p struct{ Detail string }
		json.NewDecoder(resp.Body).Decode(&p)
		return errors.New("POST " + path + ": " + strconv.Itoa(resp.StatusCode) + " " + p.Detail)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
