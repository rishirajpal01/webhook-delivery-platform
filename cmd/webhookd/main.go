// Command webhookd runs the webhook delivery service.
//
// Phase 0 only connects to Postgres, applies migrations and serves health
// checks. Delivery arrives in phase 1.
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
	logger.Info("migrations up to date", "applied", applied)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.NewHandler(db, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		logger.Info("api listening", "addr", cfg.ListenAddr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
