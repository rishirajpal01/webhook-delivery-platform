// Package config loads service settings from environment variables.
package config

import (
	"errors"
	"time"
)

// Config holds the settings webhookd needs to start.
type Config struct {
	// DatabaseURL is a Postgres connection string. Required.
	DatabaseURL string
	// ListenAddr is the address the HTTP API listens on.
	ListenAddr string
	// ShutdownTimeout bounds how long in-flight requests get to finish on exit.
	ShutdownTimeout time.Duration
}

// Load reads the config through getenv (usually os.Getenv), applying defaults
// for anything optional that is unset.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		DatabaseURL:     getenv("DATABASE_URL"),
		ListenAddr:      getenv("LISTEN_ADDR"),
		ShutdownTimeout: 10 * time.Second,
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
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
	return cfg, nil
}
