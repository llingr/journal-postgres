// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package database opens the service's Postgres pools and applies the journal's schema
package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/llingr/llingr-pgx/connect"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/config"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model/constants"
)

// ApplicationConnection is the pool request handling uses; it has no DDL privileges
func ApplicationConnection(ctx context.Context, pg config.Postgres) (*pgxpool.Pool, error) {
	pool, err := roleConnectionBuilder(pg, pg.Roles.App).Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres as %s: %w", pg.Roles.App.Username, err)
	}
	return pool, nil
}

// ownerConnection runs DDL and is only opened to migrate
func ownerConnection(ctx context.Context, pg config.Postgres) (*pgxpool.Pool, error) {
	pool, err := roleConnectionBuilder(pg, pg.Roles.Owner).Connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres as %s: %w", pg.Roles.Owner.Username, err)
	}
	return pool, nil
}

// roleConnectionBuilder holds the settings every role's pool shares
func roleConnectionBuilder(pg config.Postgres, role config.Role) *connect.ConnectionBuilder {
	return connect.NewConnectionBuilder().
		WithHost(pg.Connection.Host).
		WithPort(pg.Connection.Port).
		WithDatabase(pg.Connection.DatabaseName).
		WithSSLMode(pg.Connection.SSLMode).
		WithUser(role.Username.String()).
		WithPassword(role.Password).
		WithConnectTimeout(pg.Connection.ConnectTimeout).
		WithMaxConnIdleTime(pg.Connection.MaxIdleTime).
		WithMaxConns(role.MaxConnections).
		WithApplicationName(constants.ApplicationName)
}
