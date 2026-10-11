// Package config loads service settings from environment variables.
package config

import (
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
	// APIKey authenticates calls to /v1. Required when the role serves the API.
	APIKey string
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
		APIKey:                getenv("API_KEY"),
		ShutdownTimeout:       10 * time.Second,
		DispatcherConcurrency: 16,
		DispatcherLease:       30 * time.Second,
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
	if cfg.RunsAPI() && len(cfg.APIKey) < 16 {
		return Config{}, errors.New("API_KEY is required and must be at least 16 characters")
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
	return cfg, nil
}
