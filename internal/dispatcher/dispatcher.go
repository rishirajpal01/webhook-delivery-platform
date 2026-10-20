// Package dispatcher sends due deliveries and records the outcome.
//
// A dispatcher claims due rows with a lease, sends each one outside any
// transaction, then records the attempt and the next state. If it dies
// mid-send the lease expires, a sweeper puts the row back to pending, and
// some dispatcher sends it again. That is why delivery is at-least-once.
package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/signing"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/ssrf"
)

// maxResponseBody is how much of a receiver's response is kept in the
// attempt log.
const maxResponseBody = 4 << 10

var (
	attemptsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhookd_delivery_attempts_total",
		Help: "Delivery attempts by outcome: succeeded, retry or dead.",
	}, []string{"outcome"})
	attemptDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhookd_delivery_attempt_duration_seconds",
		Help:    "Time from sending a request to receiving the response or an error.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 13), // 5ms to ~20s
	})
	deliveryLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhookd_delivery_lag_seconds",
		Help:    "Time from a delivery becoming due to a dispatcher claiming it.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 16), // 10ms to ~5.5min
	})
	inFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "webhookd_deliveries_in_flight",
		Help: "Deliveries this process is sending right now.",
	})
	circuitTrips = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhookd_endpoint_circuit_trips_total",
		Help: "Times an endpoint's circuit breaker opened after repeated failures.",
	})
	endpointsDisabled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhookd_endpoints_disabled_total",
		Help: "Endpoints disabled after failing continuously.",
	})
)

// maxRetryAfter caps how long a receiver's Retry-After header can push a
// retry back.
const maxRetryAfter = 24 * time.Hour

// Config tunes a dispatcher. Zero values take the defaults noted.
type Config struct {
	Concurrency    int             // parallel sends per process; default 16
	PollInterval   time.Duration   // wait when no work is due; default 250ms
	Lease          time.Duration   // how long a claim is held; default 30s
	RequestTimeout time.Duration   // per HTTP request; default 15s
	Schedule       []time.Duration // retry delays; default DefaultSchedule
	// Jitter is the fraction each retry delay moves either way at random;
	// nil means DefaultJitter, and 0 turns jitter off.
	Jitter *float64
	// AllowPrivateNetworks turns off the SSRF guard. Local development only.
	AllowPrivateNetworks bool
	// AttemptRetentionDays is how many days of the attempt log to keep;
	// 0 keeps everything.
	AttemptRetentionDays int
	// MessageRetentionDays is how many days to keep messages whose
	// deliveries have all finished; 0 keeps everything.
	MessageRetentionDays int
	// Listen makes the dispatcher LISTEN for new work so it starts sending
	// at once rather than on the next poll.
	Listen bool
	// Health decides when an endpoint's circuit breaker trips and when it is
	// disabled. Defaults: trip after 10 failures in a row, cool down for 1m
	// doubling to 1h between probes, disable after 5 days of failure.
	Health postgres.HealthPolicy
}

func (c *Config) setDefaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 16
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 250 * time.Millisecond
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 15 * time.Second
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.Schedule == nil {
		c.Schedule = DefaultSchedule
	}
	if c.Jitter == nil {
		j := DefaultJitter
		c.Jitter = &j
	}
	if c.Health.FailureThreshold <= 0 {
		c.Health.FailureThreshold = 10
	}
	if c.Health.CooldownBase <= 0 {
		c.Health.CooldownBase = time.Minute
	}
	if c.Health.CooldownMax <= 0 {
		c.Health.CooldownMax = time.Hour
	}
	if c.Health.DisableAfter <= 0 {
		c.Health.DisableAfter = 5 * 24 * time.Hour
	}
}

// Dispatcher claims and sends deliveries.
type Dispatcher struct {
	store     *postgres.Store
	cfg       Config
	client    *http.Client
	logger    *slog.Logger
	rnd       func() float64
	slotFreed chan struct{}
	// due is signalled when Postgres notifies that new work may be due.
	due chan struct{}
}

// New returns a dispatcher. Call Run to start it.
func New(store *postgres.Store, cfg Config, logger *slog.Logger) *Dispatcher {
	cfg.setDefaults()
	return &Dispatcher{
		store:     store,
		cfg:       cfg,
		client:    newHTTPClient(cfg.RequestTimeout, cfg.Concurrency, cfg.AllowPrivateNetworks),
		logger:    logger,
		rnd:       rand.Float64,
		slotFreed: make(chan struct{}, 1),
		due:       make(chan struct{}, 1),
	}
}

func newHTTPClient(timeout time.Duration, concurrency int, allowPrivate bool) *http.Client {
	dialer := ssrf.NewDialer(5*time.Second, allowPrivate)
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:             nil, // a proxy would connect on our behalf and bypass the SSRF check
			DialContext:       dialer.DialContext,
			ForceAttemptHTTP2: true,
			// Up to every slot may be sending to one host; keep those
			// connections for reuse instead of reconnecting each time.
			MaxIdleConnsPerHost:   concurrency,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: timeout,
		},
		// Redirects are not followed: a 3xx counts as a failure. Following
		// them would let a receiver point us at an address it chose later.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// Run claims and sends deliveries until ctx is cancelled, then waits for the
// sends already started to finish (each is bounded by RequestTimeout).
func (d *Dispatcher) Run(ctx context.Context) error {
	d.logger.Info("dispatcher started", "concurrency", d.cfg.Concurrency, "lease", d.cfg.Lease.String())
	// Attempts can only be recorded on days that have a partition, so make
	// sure they exist before sending anything.
	d.maintainPartitions(ctx)

	sem := make(chan struct{}, d.cfg.Concurrency)
	minBatch := max(1, d.cfg.Concurrency/4)
	var background sync.WaitGroup
	defer func() {
		// Fill every slot: this returns once all running sends are done.
		for range cap(sem) {
			sem <- struct{}{}
		}
		background.Wait()
		d.logger.Info("dispatcher stopped")
	}()

	background.Go(func() { d.releaseExpiredLoop(ctx) })
	if d.cfg.Listen {
		background.Go(func() { d.listenLoop(ctx) })
	}
	background.Go(func() {
		for ; ctx.Err() == nil; sleep(ctx, time.Hour) {
			d.maintain(ctx)
		}
	})

	for ctx.Err() == nil {
		// Only this loop adds to sem, so `free` slots are guaranteed free.
		// While sends are running, wait until a quarter of the slots are
		// free: claiming one delivery per finished send would make the
		// claim queries, not the sends, the limit on throughput.
		free := cap(sem) - len(sem)
		if free == 0 || (free < minBatch && len(sem) > 0) {
			select {
			case <-ctx.Done():
			case <-d.slotFreed:
			}
			continue
		}

		jobs, err := d.claim(ctx, free)
		if err != nil {
			if ctx.Err() == nil {
				d.logger.Error("claim failed", "err", err)
			}
			sleep(ctx, d.cfg.PollInterval)
			continue
		}
		claimedAt := time.Now()
		for _, j := range jobs {
			deliveryLag.Observe(claimedAt.Sub(j.DueAt).Seconds())
			sem <- struct{}{}
			go func() {
				defer func() {
					<-sem
					select {
					case d.slotFreed <- struct{}{}:
					default:
					}
				}()
				d.process(context.WithoutCancel(ctx), j)
			}()
		}
		if len(jobs) < free {
			// Nothing else is claimable now. Wait for work to come due, or
			// for a send to finish: that may free an endpoint's slot.
			t := time.NewTimer(d.cfg.PollInterval)
			select {
			case <-ctx.Done():
			case <-d.slotFreed:
			case <-d.due:
			case <-t.C:
			}
			t.Stop()
		}
	}
	d.logger.Info("dispatcher draining", "in_flight", len(sem))
	return nil
}

// claim fills up to free slots: probes for endpoints whose circuit breaker
// is ready for one first, then ordinary due deliveries.
func (d *Dispatcher) claim(ctx context.Context, free int) ([]postgres.Job, error) {
	jobs, err := d.store.ClaimProbes(ctx, free, d.cfg.Lease)
	if err != nil {
		return jobs, err
	}
	if rest := free - len(jobs); rest > 0 {
		more, err := d.store.Claim(ctx, rest, d.cfg.Lease)
		jobs = append(jobs, more...)
		if err != nil {
			return jobs, err
		}
	}
	return jobs, nil
}

// maintain creates the coming days' attempt log partitions, drops the ones
// past retention, and deletes finished messages past retention.
func (d *Dispatcher) maintain(ctx context.Context) {
	d.maintainPartitions(ctx)
	if d.cfg.MessageRetentionDays <= 0 {
		return
	}
	n, err := d.store.DeleteOldMessages(ctx, d.cfg.MessageRetentionDays, 1000)
	if err != nil && ctx.Err() == nil {
		d.logger.Error("message retention failed", "err", err)
	}
	if n > 0 {
		d.logger.Info("old messages deleted", "count", n)
	}
}

func (d *Dispatcher) maintainPartitions(ctx context.Context) {
	created, dropped, err := d.store.MaintainAttemptPartitions(ctx, d.cfg.AttemptRetentionDays)
	if err != nil {
		if ctx.Err() == nil {
			d.logger.Error("attempt partition maintenance failed", "err", err)
		}
		return
	}
	if created > 0 || dropped > 0 {
		d.logger.Info("attempt partitions maintained", "created", created, "dropped", dropped)
	}
}

// listenLoop keeps a LISTEN connection open so new messages and replays wake
// the claim loop at once; the poll still runs in case a notification is
// missed while reconnecting.
func (d *Dispatcher) listenLoop(ctx context.Context) {
	wake := func() {
		select {
		case d.due <- struct{}{}:
		default:
		}
	}
	for ctx.Err() == nil {
		err := d.store.ListenDue(ctx, wake)
		if ctx.Err() != nil {
			return
		}
		d.logger.Warn("listen for new work failed; polling until it reconnects", "err", err)
		sleep(ctx, 5*time.Second)
	}
}

func (d *Dispatcher) releaseExpiredLoop(ctx context.Context) {
	interval := max(d.cfg.Lease/3, time.Second)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := d.store.ReleaseExpired(ctx)
		if err != nil && ctx.Err() == nil {
			d.logger.Error("release expired leases failed", "err", err)
		}
		if n > 0 {
			d.logger.Warn("released expired leases", "count", n)
		}
	}
}

// process sends one delivery and records the result. ctx is not cancelled
// on shutdown, so a send in progress is finished rather than abandoned.
func (d *Dispatcher) process(ctx context.Context, j postgres.Job) {
	inFlight.Inc()
	defer inFlight.Dec()

	attempt, retryAfter := d.send(ctx, j)
	ok := attempt.StatusCode != nil && *attempt.StatusCode >= 200 && *attempt.StatusCode < 300

	o := postgres.Outcome{Attempt: attempt}
	switch delay, retry := retryDelay(d.cfg.Schedule, j.AttemptCount+1, *d.cfg.Jitter, d.rnd); {
	case ok:
		o.NextState = postgres.DeliverySucceeded
	case retry:
		// Never come back sooner than the receiver asked.
		delay = max(delay, min(retryAfter, maxRetryAfter))
		o.NextState = postgres.DeliveryPending
		o.RetryAt = time.Now().Add(delay)
	default:
		o.NextState = postgres.DeliveryDead
	}

	outcome := map[string]string{
		postgres.DeliverySucceeded: "succeeded",
		postgres.DeliveryPending:   "retry",
		postgres.DeliveryDead:      "dead",
	}[o.NextState]
	attemptsTotal.WithLabelValues(outcome).Inc()

	log := d.logger.With("delivery_id", j.DeliveryID, "message_id", j.MessageID,
		"endpoint_id", j.EndpointID, "attempt", j.AttemptCount+1, "outcome", outcome, "probe", j.Probe)
	if !ok {
		log.Info("delivery attempt failed", "status", attempt.StatusCode, "err", attempt.Error)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r, err := d.store.Complete(ctx, j, o, d.cfg.Health)
	if err != nil {
		// The lease will expire and the delivery will be sent again.
		log.Error("recording attempt failed", "err", err)
		return
	}
	if !r.Applied {
		log.Warn("lease was lost before the attempt was recorded; another dispatcher owns the delivery")
	}
	if r.Tripped {
		circuitTrips.Inc()
		log.Warn("endpoint circuit breaker opened", "consecutive_failures", d.cfg.Health.FailureThreshold)
	}
	if r.Disabled {
		endpointsDisabled.Inc()
		log.Warn("endpoint disabled after failing continuously", "after", d.cfg.Health.DisableAfter.String(),
			"event_message_id", r.DisabledEvent)
	}
}

// send makes one signed HTTP request and describes what happened. It also
// returns how long a 429 or 503 response asked us to wait (0 if it did not).
func (d *Dispatcher) send(ctx context.Context, j postgres.Job) (postgres.Attempt, time.Duration) {
	start := time.Now()
	a := postgres.Attempt{DeliveryID: j.DeliveryID, AttemptedAt: start}
	fail := func(err error) (postgres.Attempt, time.Duration) {
		a.LatencyMS = int(time.Since(start).Milliseconds())
		a.Error = err.Error()
		attemptDuration.Observe(time.Since(start).Seconds())
		return a, 0
	}

	body, err := envelope(j)
	if err != nil {
		return fail(err)
	}
	sig, err := signing.Sign(j.Secret, j.MessageID, start, body)
	if err != nil {
		return fail(err)
	}
	if j.PreviousSecret != "" {
		// During rotation, sign with the old secret too; receivers accept a
		// request if any signature matches.
		old, err := signing.Sign(j.PreviousSecret, j.MessageID, start, body)
		if err != nil {
			return fail(err)
		}
		sig += " " + old
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, j.URL, bytes.NewReader(body))
	if err != nil {
		return fail(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "webhookd")
	req.Header.Set(signing.HeaderID, j.MessageID)
	req.Header.Set(signing.HeaderTimestamp, strconv.FormatInt(start.Unix(), 10))
	req.Header.Set(signing.HeaderSignature, sig)

	resp, err := d.client.Do(req)
	if err != nil {
		return fail(err)
	}
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	// Drain a little more so the connection can be reused, then close.
	io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()

	a.LatencyMS = int(time.Since(start).Milliseconds())
	attemptDuration.Observe(time.Since(start).Seconds())
	code := resp.StatusCode
	a.StatusCode = &code
	// Postgres text cannot hold invalid UTF-8 or NUL bytes.
	a.ResponseBody = strings.ReplaceAll(string(bytes.ToValidUTF8(respBody, []byte("\uFFFD"))), "\x00", "")

	var retryAfter time.Duration
	if code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable {
		retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	}
	return a, retryAfter
}

// parseRetryAfter reads a Retry-After header, which is either a number of
// seconds or an HTTP date. It returns 0 for anything else.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// envelope is the request body: the payload structure Standard Webhooks
// recommends, so the event type is covered by the signature. It is the same
// on every attempt, so receivers can deduplicate on webhook-id.
func envelope(j postgres.Job) ([]byte, error) {
	return json.Marshal(struct {
		Type      string          `json:"type"`
		Timestamp time.Time       `json:"timestamp"`
		Data      json.RawMessage `json:"data"`
	}{j.EventType, j.CreatedAt.UTC(), j.Payload})
}
