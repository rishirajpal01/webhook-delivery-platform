// Package config loads service settings from environment variables.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Roles a process can run.
const (
	RoleAPI        = "api"
	RoleDispatcher = "dispatcher"
	RoleAll        = "all"
)

// Config holds the settings webhookd needs to start.
type Config struct {
	// DatabaseURL is a Postgres connection string. Required.
	DatabaseURL string
	// DBMaxConns caps this process's Postgres connections. Keep the sum over
	// every process below the server's max_connections.
	DBMaxConns int
	// Role is api, dispatcher or all (both in one process).
	Role string
	// ListenAddr is the address the HTTP API listens on.
	ListenAddr string
	// APIKeyHash is the SHA-256 of the API key that authenticates calls to
	// /v1, from API_KEY_SHA256 or by hashing API_KEY. Only the hash is kept,
	// so the key itself never has to be stored. Required when the role
	// serves the API.
	APIKeyHash [32]byte
	// ShutdownTimeout bounds how long in-flight requests get to finish on exit.
	ShutdownTimeout time.Duration
	// SecretRotationOverlap is how long a rotated-out secret keeps signing
	// requests alongside the new one.
	SecretRotationOverlap time.Duration
	// DispatcherConcurrency is how many deliveries one process sends at once.
	DispatcherConcurrency int
	// DispatcherLease is how long a claimed delivery stays reserved before
	// another dispatcher may retry it. It must be longer than
	// DispatcherRequestTimeout, or slow sends get duplicated.
	DispatcherLease time.Duration
	// DispatcherRequestTimeout bounds one HTTP request to a receiver.
	DispatcherRequestTimeout time.Duration
	// DispatcherPollInterval is how often an idle dispatcher looks for due
	// work when no notification wakes it sooner.
	DispatcherPollInterval time.Duration
	// DispatcherListen makes dispatchers LISTEN for new work so they start
	// at once instead of on the next poll. Turn it off behind a pooler in
	// transaction mode (such as PgBouncer), which does not support LISTEN.
	DispatcherListen bool
	// RetrySchedule is the wait before each retry; its length plus one is
	// the number of attempts. Nil means the dispatcher's default schedule.
	RetrySchedule []time.Duration
	// RetryJitter moves each retry delay by up to this fraction either way.
	RetryJitter float64
	// CircuitFailureThreshold is how many failures in a row trip an
	// endpoint's circuit breaker.
	CircuitFailureThreshold int
	// CircuitCooldown is the first wait before probing a tripped endpoint;
	// it doubles after each failed probe up to CircuitCooldownMax.
	CircuitCooldown, CircuitCooldownMax time.Duration
	// EndpointDisableAfter is how long an endpoint may fail continuously
	// before it is disabled.
	EndpointDisableAfter time.Duration
	// AllowPrivateNetworks lets deliveries reach private and loopback
	// addresses. For local development only; it disables the SSRF guard.
	AllowPrivateNetworks bool
	// AttemptRetentionDays is how many days of the attempt log dispatchers
	// keep; older days are dropped. 0 keeps everything.
	AttemptRetentionDays int
	// MessageRetentionDays is how many days dispatchers keep messages, and
	// with them their deliveries, once every delivery is finished. 0 keeps
	// everything.
	MessageRetentionDays int
}

// RunsAPI reports whether this process serves the HTTP API.
func (c Config) RunsAPI() bool { return c.Role == RoleAPI || c.Role == RoleAll }

// RunsDispatcher reports whether this process sends deliveries.
func (c Config) RunsDispatcher() bool { return c.Role == RoleDispatcher || c.Role == RoleAll }

// Load reads the config through getenv (usually os.Getenv), applying defaults
// for anything optional that is unset.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:              getenv("DATABASE_URL"),
		DBMaxConns:               40,
		Role:                     getenv("ROLE"),
		ListenAddr:               getenv("LISTEN_ADDR"),
		ShutdownTimeout:          10 * time.Second,
		SecretRotationOverlap:    24 * time.Hour,
		DispatcherConcurrency:    16,
		DispatcherLease:          30 * time.Second,
		DispatcherRequestTimeout: 15 * time.Second,
		DispatcherPollInterval:   250 * time.Millisecond,
		DispatcherListen:         true,
		RetryJitter:              0.25,
		CircuitFailureThreshold:  10,
		CircuitCooldown:          time.Minute,
		CircuitCooldownMax:       time.Hour,
		EndpointDisableAfter:     5 * 24 * time.Hour,
		AttemptRetentionDays:     30,
		MessageRetentionDays:     30,
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if cfg.Role == "" {
		cfg.Role = RoleAll
	}
	if cfg.Role != RoleAPI && cfg.Role != RoleDispatcher && cfg.Role != RoleAll {
		return Config{}, errors.New("ROLE must be api, dispatcher or all")
	}
	if cfg.RunsAPI() {
		h, err := apiKeyHash(getenv("API_KEY"), getenv("API_KEY_SHA256"))
		if err != nil {
			return Config{}, err
		}
		cfg.APIKeyHash = h
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}

	p := parser{getenv: getenv}
	p.int(&cfg.DBMaxConns, "DB_MAX_CONNS", 2)
	p.duration(&cfg.ShutdownTimeout, "SHUTDOWN_TIMEOUT", time.Millisecond)
	p.duration(&cfg.SecretRotationOverlap, "SECRET_ROTATION_OVERLAP", 0)
	p.int(&cfg.DispatcherConcurrency, "DISPATCHER_CONCURRENCY", 1)
	p.duration(&cfg.DispatcherLease, "DISPATCHER_LEASE", time.Second)
	p.duration(&cfg.DispatcherRequestTimeout, "DISPATCHER_REQUEST_TIMEOUT", 100*time.Millisecond)
	p.duration(&cfg.DispatcherPollInterval, "DISPATCHER_POLL_INTERVAL", 10*time.Millisecond)
	p.bool(&cfg.DispatcherListen, "DISPATCHER_LISTEN")
	p.schedule(&cfg.RetrySchedule, "RETRY_SCHEDULE")
	p.fraction(&cfg.RetryJitter, "RETRY_JITTER")
	p.int(&cfg.CircuitFailureThreshold, "CIRCUIT_FAILURE_THRESHOLD", 1)
	p.duration(&cfg.CircuitCooldown, "CIRCUIT_COOLDOWN", time.Second)
	p.duration(&cfg.CircuitCooldownMax, "CIRCUIT_COOLDOWN_MAX", time.Second)
	p.duration(&cfg.EndpointDisableAfter, "ENDPOINT_DISABLE_AFTER", time.Minute)
	p.bool(&cfg.AllowPrivateNetworks, "ALLOW_PRIVATE_NETWORKS")
	p.int(&cfg.AttemptRetentionDays, "ATTEMPT_RETENTION_DAYS", 0)
	p.int(&cfg.MessageRetentionDays, "MESSAGE_RETENTION_DAYS", 0)
	if p.err != nil {
		return Config{}, p.err
	}

	if cfg.DispatcherLease <= cfg.DispatcherRequestTimeout {
		return Config{}, fmt.Errorf("DISPATCHER_LEASE (%s) must be longer than DISPATCHER_REQUEST_TIMEOUT (%s), or slow sends are duplicated",
			cfg.DispatcherLease, cfg.DispatcherRequestTimeout)
	}
	if cfg.CircuitCooldownMax < cfg.CircuitCooldown {
		return Config{}, errors.New("CIRCUIT_COOLDOWN_MAX must be at least CIRCUIT_COOLDOWN")
	}
	return cfg, nil
}

// parser reads optional settings, keeping the first error.
type parser struct {
	getenv func(string) string
	err    error
}

func (p *parser) fail(format string, args ...any) {
	if p.err == nil {
		p.err = fmt.Errorf(format, args...)
	}
}

// int sets *dst from name if it is set; the value must be at least min.
func (p *parser) int(dst *int, name string, min int) {
	if v := p.getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < min {
			p.fail("%s must be a whole number of at least %d", name, min)
			return
		}
		*dst = n
	}
}

// duration sets *dst from name if it is set; the value must be at least
// min (a negative or zero duration is refused whenever min is positive).
func (p *parser) duration(dst *time.Duration, name string, min time.Duration) {
	if v := p.getenv(name); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < min {
			p.fail("%s must be a duration such as 30s of at least %s", name, min)
			return
		}
		*dst = d
	}
}

func (p *parser) bool(dst *bool, name string) {
	if v := p.getenv(name); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			p.fail("%s must be true or false", name)
			return
		}
		*dst = b
	}
}

// fraction sets *dst from name if it is set; the value must be in [0, 1].
func (p *parser) fraction(dst *float64, name string) {
	if v := p.getenv(name); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || f > 1 {
			p.fail("%s must be a number from 0 to 1", name)
			return
		}
		*dst = f
	}
}

// schedule sets *dst from a comma-separated list of positive durations.
func (p *parser) schedule(dst *[]time.Duration, name string) {
	v := p.getenv(name)
	if v == "" {
		return
	}
	var out []time.Duration
	for _, s := range strings.Split(v, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil || d <= 0 {
			p.fail("%s must be a comma-separated list of positive durations such as 5s,1m,1h", name)
			return
		}
		out = append(out, d)
	}
	*dst = out
}

// apiKeyHash returns the SHA-256 of the API key, given either the key or its
// hash in hex (as printed by "printf %s KEY | sha256sum"), but not both.
func apiKeyHash(key, hexHash string) ([32]byte, error) {
	var h [32]byte
	switch {
	case key != "" && hexHash != "":
		return h, errors.New("set API_KEY or API_KEY_SHA256, not both")
	case hexHash != "":
		b, err := hex.DecodeString(hexHash)
		if err != nil || len(b) != len(h) {
			return h, errors.New("API_KEY_SHA256 must be 64 hex characters")
		}
		copy(h[:], b)
		return h, nil
	case len(key) < 16:
		return h, errors.New("API_KEY (at least 16 characters) or API_KEY_SHA256 is required")
	}
	return sha256.Sum256([]byte(key)), nil
}
