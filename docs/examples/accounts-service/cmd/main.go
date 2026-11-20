// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Command accounts-service is a complete application on the journal library: an HTTP service
// keeping each account as a stream of events in Postgres, built on anvil, anvil-koanf,
// anvil-zap and llingr-pgx.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/llingr/anvil"
	"github.com/llingr/anvil-zap/zaplog"
	"github.com/llingr/anvil/shutdown"
	"go.uber.org/zap"

	"github.com/llingr/journal"
	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal/publish"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/cmd/database"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/config"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/controller"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model/constants"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/repos/accounts_repo"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/services/accounts"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/services/publishing"
)

func main() {
	loggerProvider := zaplog.New(zaplog.DefaultConfig())
	configProvider := config.NewProvider()

	exitCode := anvil.Run(context.Background(), constants.ApplicationName, configProvider,
		loggerProvider, wire)
	os.Exit(exitCode)
}

// wire builds the service top to bottom; shutdown groups stop last added first, so the HTTP
// server stops, then the journal's sweeps, then the database pool closes
func wire(ctx context.Context, shell anvil.Shell[config.Config, *zap.Logger]) error {
	// static config
	cfg := shell.Config()
	logger := shell.Logger()

	// persistence
	appPool, err := database.ApplicationConnection(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	shell.AddShutdownGroup(shutdown.Named("postgres", shutdown.Close(appPool.Close)))

	err = database.Migrate(ctx, cfg.Postgres)
	if err != nil {
		return err
	}

	eventStore := journal.NewStore(postgres.NewBackend(appPool),
		journal.WithPublisher(publishing.New(logger)),
		journal.WithHooks(publish.Hooks{
			Failed: func(streamID uuid.UUID, publishErr error) {
				logger.Warn("publish failed, retried later", zap.Stringer("streamId", streamID),
					zap.Error(publishErr))
			},
		}),
	)
	eventStore.Start(ctx)
	shell.AddShutdownGroup(shutdown.Named("journal", eventStore))
	accountsRepo := accounts_repo.New(eventStore)

	// services
	accountService := accounts.New(accountsRepo)

	// web
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	controller.RegisterHandlers(engine, accountService, shell.Stopping, logger)
	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:           engine,
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
	}
	shell.AddShutdownGroup(server).SetName("http").Go(func(context.Context) error {
		return server.ListenAndServe()
	})

	logger.Info("listening", zap.Int("port", cfg.Server.Port))
	return nil
}
