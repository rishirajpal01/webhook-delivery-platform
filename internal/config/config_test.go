package config

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
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
	if cfg.APIKeyHash != sha256.Sum256([]byte(key)) {
		t.Error("APIKeyHash is not the SHA-256 of API_KEY")
	}
	if cfg.ListenAddr != ":8080" || cfg.ShutdownTimeout != 10*time.Second ||
		cfg.DispatcherConcurrency != 16 || cfg.DispatcherLease != 30*time.Second || cfg.AllowPrivateNetworks ||
		cfg.AttemptRetentionDays != 30 || cfg.MessageRetentionDays != 30 || cfg.DBMaxConns != 40 ||
		cfg.DispatcherRequestTimeout != 15*time.Second || cfg.DispatcherPollInterval != 250*time.Millisecond ||
		!cfg.DispatcherListen || cfg.RetrySchedule != nil || cfg.RetryJitter != 0.25 ||
		cfg.CircuitFailureThreshold != 10 || cfg.CircuitCooldown != time.Minute || cfg.CircuitCooldownMax != time.Hour ||
		cfg.EndpointDisableAfter != 120*time.Hour || cfg.SecretRotationOverlap != 24*time.Hour {
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

		"DB_MAX_CONNS":               "10",
		"MESSAGE_RETENTION_DAYS":     "7",
		"SECRET_ROTATION_OVERLAP":    "1h",
		"DISPATCHER_REQUEST_TIMEOUT": "4s",
		"DISPATCHER_POLL_INTERVAL":   "1s",
		"DISPATCHER_LISTEN":          "false",
		"RETRY_SCHEDULE":             "1s, 1m,1h",
		"RETRY_JITTER":               "0",
		"CIRCUIT_FAILURE_THRESHOLD":  "3",
		"CIRCUIT_COOLDOWN":           "10s",
		"CIRCUIT_COOLDOWN_MAX":       "10m",
		"ENDPOINT_DISABLE_AFTER":     "24h",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RunsAPI() || !cfg.RunsDispatcher() || cfg.ListenAddr != ":9999" ||
		cfg.ShutdownTimeout != 3*time.Second || cfg.DispatcherConcurrency != 4 || cfg.DispatcherLease != 5*time.Second || !cfg.AllowPrivateNetworks ||
		cfg.AttemptRetentionDays != 0 {
		t.Errorf("got %+v", cfg)
	}
	if cfg.DBMaxConns != 10 || cfg.MessageRetentionDays != 7 || cfg.SecretRotationOverlap != time.Hour ||
		cfg.DispatcherRequestTimeout != 4*time.Second || cfg.DispatcherPollInterval != time.Second || cfg.DispatcherListen ||
		!slices.Equal(cfg.RetrySchedule, []time.Duration{time.Second, time.Minute, time.Hour}) || cfg.RetryJitter != 0 ||
		cfg.CircuitFailureThreshold != 3 || cfg.CircuitCooldown != 10*time.Second || cfg.CircuitCooldownMax != 10*time.Minute ||
		cfg.EndpointDisableAfter != 24*time.Hour {
		t.Errorf("got %+v", cfg)
	}
}

func TestLoadAPIKeyHash(t *testing.T) {
	sum := sha256.Sum256([]byte(key))
	cfg, err := Load(env(map[string]string{"DATABASE_URL": "postgres://x", "API_KEY_SHA256": hex.EncodeToString(sum[:])}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIKeyHash != sum {
		t.Error("APIKeyHash does not match API_KEY_SHA256")
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
		"key and hash":         with("API_KEY_SHA256", "00"),
		"bad hash":             map[string]string{"DATABASE_URL": "postgres://x", "API_KEY_SHA256": "xyz"},
		"bad role":             with("ROLE", "worker"),
		"bad timeout":          with("SHUTDOWN_TIMEOUT", "soon"),
		"negative timeout":     with("SHUTDOWN_TIMEOUT", "-1s"),
		"bad concurrency":      with("DISPATCHER_CONCURRENCY", "0"),
		"short lease":          with("DISPATCHER_LEASE", "10ms"),
		"bad bool":             with("ALLOW_PRIVATE_NETWORKS", "maybe"),
		"negative retention":   with("ATTEMPT_RETENTION_DAYS", "-1"),
		"lease below timeout":  with("DISPATCHER_LEASE", "15s"),
		"one connection":       with("DB_MAX_CONNS", "1"),
		"bad schedule":         with("RETRY_SCHEDULE", "5s,,1m"),
		"negative delay":       with("RETRY_SCHEDULE", "-5s"),
		"jitter above one":     with("RETRY_JITTER", "1.5"),
		"cooldown above max":   with("CIRCUIT_COOLDOWN", "2h"),
		"zero threshold":       with("CIRCUIT_FAILURE_THRESHOLD", "0"),
		"negative overlap":     with("SECRET_ROTATION_OVERLAP", "-1h"),
		"bad listen":           with("DISPATCHER_LISTEN", "sometimes"),
	}
	for name, vars := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(env(vars)); err == nil {
				t.Error("expected an error")
			}
		})
	}
}
