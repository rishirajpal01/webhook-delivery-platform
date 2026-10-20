// Command webhookd runs the webhook delivery service.
//
// ROLE picks what the process does: "api" serves the HTTP API, "dispatcher"
// sends deliveries, and "all" (the default) does both. Any number of each can
// run against one Postgres.
//
// "webhookd healthcheck" asks a running webhookd on LISTEN_ADDR whether it
// is ready and exits 0 if so, for container health checks (the image has no
// shell or curl).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/api"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/config"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/dispatcher"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck(os.Getenv("LISTEN_ADDR")))
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("webhookd exited", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := postgres.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if err != nil {
		return err
	}
	defer db.Close()

	applied, err := postgres.Migrate(ctx, db)
	if err != nil {
		return err
	}
	logger.Info("migrations up to date", "applied", applied, "role", cfg.Role)

	store := postgres.NewStore(db)
	errc := make(chan error, 2)
	running := 0

	// Every role serves health checks and metrics; only the api role serves /v1.
	handler := api.NewOpsHandler(db, logger)
	if cfg.RunsAPI() {
		handler = api.NewHandler(db, store, cfg.APIKeyHash, logger, api.WithRotationOverlap(cfg.SecretRotationOverlap))
	}
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	running++
	go func() {
		logger.Info("http listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errc <- err
			return
		}
		errc <- nil
	}()

	if cfg.RunsDispatcher() {
		d := dispatcher.New(store, dispatcher.Config{
			Concurrency:          cfg.DispatcherConcurrency,
			Lease:                cfg.DispatcherLease,
			RequestTimeout:       cfg.DispatcherRequestTimeout,
			PollInterval:         cfg.DispatcherPollInterval,
			Listen:               cfg.DispatcherListen,
			Schedule:             cfg.RetrySchedule,
			Jitter:               &cfg.RetryJitter,
			AllowPrivateNetworks: cfg.AllowPrivateNetworks,
			AttemptRetentionDays: cfg.AttemptRetentionDays,
			MessageRetentionDays: cfg.MessageRetentionDays,
			Health: postgres.HealthPolicy{
				FailureThreshold: cfg.CircuitFailureThreshold,
				CooldownBase:     cfg.CircuitCooldown,
				CooldownMax:      cfg.CircuitCooldownMax,
				DisableAfter:     cfg.EndpointDisableAfter,
			},
		}, logger)
		if cfg.AllowPrivateNetworks {
			logger.Warn("SSRF guard is off: deliveries may reach private networks")
		}
		running++
		go func() { errc <- d.Run(ctx) }()
	}

	// Run until a signal arrives or a component fails.
	var firstErr error
	select {
	case <-ctx.Done():
	case firstErr = <-errc:
		running--
	}
	stop() // a failure stops the other component too

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && firstErr == nil {
		firstErr = err
	}
	for ; running > 0; running-- {
		if err := <-errc; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// healthcheck returns 0 if the webhookd listening on addr (":8080" when
// empty) answers /readyz with 200.
func healthcheck(addr string) int {
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get("http://" + addr + "/readyz")
	if err != nil {
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
