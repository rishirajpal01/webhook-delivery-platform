// Command webhookd runs the webhook delivery service.
//
// ROLE picks what the process does: "api" serves the HTTP API, "dispatcher"
// sends deliveries, and "all" (the default) does both. Any number of each can
// run against one Postgres.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/internal/api"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/config"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/dispatcher"
	"github.com/rishirajpal01/webhook-delivery-platform/internal/postgres"
)

func main() {
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

	db, err := postgres.Open(ctx, cfg.DatabaseURL)
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
		handler = api.NewHandler(db, store, cfg.APIKey, logger)
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
			AllowPrivateNetworks: cfg.AllowPrivateNetworks,
			AttemptRetentionDays: cfg.AttemptRetentionDays,
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
