// Package config loads service settings from environment variables.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strconv"
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
	// DispatcherConcurrency is how many deliveries one process sends at once.
	DispatcherConcurrency int
	// DispatcherLease is how long a claimed delivery stays reserved before
	// another dispatcher may retry it. Keep it above the 15s request timeout,
	// or slow sends get duplicated; it is short only in crash tests.
	DispatcherLease time.Duration
	// AllowPrivateNetworks lets deliveries reach private and loopback
	// addresses. For local development only; it disables the SSRF guard.
	AllowPrivateNetworks bool
	// AttemptRetentionDays is how many days of the attempt log dispatchers
	// keep; older days are dropped. 0 keeps everything.
	AttemptRetentionDays int
}

// RunsAPI reports whether this process serves the HTTP API.
func (c Config) RunsAPI() bool { return c.Role == RoleAPI || c.Role == RoleAll }

// RunsDispatcher reports whether this process sends deliveries.
func (c Config) RunsDispatcher() bool { return c.Role == RoleDispatcher || c.Role == RoleAll }

// Load reads the config through getenv (usually os.Getenv), applying defaults
// for anything optional that is unset.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:           getenv("DATABASE_URL"),
		Role:                  getenv("ROLE"),
		ListenAddr:            getenv("LISTEN_ADDR"),
		ShutdownTimeout:       10 * time.Second,
		DispatcherConcurrency: 16,
		DispatcherLease:       30 * time.Second,
		AttemptRetentionDays:  30,
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
	if v := getenv("SHUTDOWN_TIMEOUT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return Config{}, errors.New("SHUTDOWN_TIMEOUT must be a positive duration such as 10s")
		}
		cfg.ShutdownTimeout = d
	}
	if v := getenv("DISPATCHER_CONCURRENCY"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, errors.New("DISPATCHER_CONCURRENCY must be a positive integer")
		}
		cfg.DispatcherConcurrency = n
	}
	if v := getenv("DISPATCHER_LEASE"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second {
			return Config{}, errors.New("DISPATCHER_LEASE must be a duration of at least 1s")
		}
		cfg.DispatcherLease = d
	}
	if v := getenv("ALLOW_PRIVATE_NETWORKS"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, errors.New("ALLOW_PRIVATE_NETWORKS must be true or false")
		}
		cfg.AllowPrivateNetworks = b
	}
	if v := getenv("ATTEMPT_RETENTION_DAYS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return Config{}, errors.New("ATTEMPT_RETENTION_DAYS must be a whole number of days, 0 to keep everything")
		}
		cfg.AttemptRetentionDays = n
	}
	return cfg, nil
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
