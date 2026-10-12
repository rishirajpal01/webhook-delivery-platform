package config

import (
	"testing"
	"time"
)

const key = "0123456789abcdef"

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"DATABASE_URL": "postgres://x", "API_KEY": key}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Role != RoleAll || !cfg.RunsAPI() || !cfg.RunsDispatcher() {
		t.Errorf("Role = %q, want all", cfg.Role)
	}
	if cfg.ListenAddr != ":8080" || cfg.ShutdownTimeout != 10*time.Second ||
		cfg.DispatcherConcurrency != 16 || cfg.DispatcherLease != 30*time.Second || cfg.AllowPrivateNetworks ||
		cfg.AttemptRetentionDays != 30 {
		t.Errorf("unexpected defaults %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"DATABASE_URL":           "postgres://x",
		"ROLE":                   "dispatcher",
		"LISTEN_ADDR":            ":9999",
		"SHUTDOWN_TIMEOUT":       "3s",
		"DISPATCHER_CONCURRENCY": "4",
		"DISPATCHER_LEASE":       "5s",
		"ALLOW_PRIVATE_NETWORKS": "true",
		"ATTEMPT_RETENTION_DAYS": "0",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RunsAPI() || !cfg.RunsDispatcher() || cfg.ListenAddr != ":9999" ||
		cfg.ShutdownTimeout != 3*time.Second || cfg.DispatcherConcurrency != 4 || cfg.DispatcherLease != 5*time.Second || !cfg.AllowPrivateNetworks ||
		cfg.AttemptRetentionDays != 0 {
		t.Errorf("got %+v", cfg)
	}
}

func TestLoadErrors(t *testing.T) {
	ok := map[string]string{"DATABASE_URL": "postgres://x", "API_KEY": key}
	with := func(k, v string) map[string]string {
		m := map[string]string{}
		for a, b := range ok {
			m[a] = b
		}
		m[k] = v
		return m
	}
	tests := map[string]map[string]string{
		"missing database url": with("DATABASE_URL", ""),
		"missing api key":      with("API_KEY", ""),
		"short api key":        with("API_KEY", "short"),
		"bad role":             with("ROLE", "worker"),
		"bad timeout":          with("SHUTDOWN_TIMEOUT", "soon"),
		"negative timeout":     with("SHUTDOWN_TIMEOUT", "-1s"),
		"bad concurrency":      with("DISPATCHER_CONCURRENCY", "0"),
		"short lease":          with("DISPATCHER_LEASE", "10ms"),
		"bad bool":             with("ALLOW_PRIVATE_NETWORKS", "maybe"),
		"negative retention":   with("ATTEMPT_RETENTION_DAYS", "-1"),
	}
	for name, vars := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(env(vars)); err == nil {
				t.Error("expected an error")
			}
		})
	}
}
