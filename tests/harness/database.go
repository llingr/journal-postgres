// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package harness provides the real-Postgres test rig for the event store schema:
// per-test isolated schemas, one connection pool per database role, and the
// migration run that puts the schema under test in place.
package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DatabaseURLVariable names the environment variable holding the superuser
	// connection string the suite bootstraps from.
	DatabaseURLVariable = "TEST_DATABASE_URL"

	// DefaultDatabaseURL points at the suite's dedicated local container.
	DefaultDatabaseURL = "postgres://postgres:pw@127.0.0.1:55440/postgres?sslmode=disable"

	// TestDatabaseName holds every per-test schema.
	TestDatabaseName = "event_store_schema_tests"

	// RolePassword is used by both test roles in the throwaway container.
	RolePassword = "pw"
)

// Role usernames created once per cluster, for the migrations' owner and app placeholders.
const (
	OwnerUsername = "event_store_owner"
	AppUsername   = "event_store_app"
)

const startContainerHint = "make test-database"

// baseDatabaseURL returns the superuser connection string.
func baseDatabaseURL() string {
	if fromEnvironment := os.Getenv(DatabaseURLVariable); fromEnvironment != "" {
		return fromEnvironment
	}
	return DefaultDatabaseURL
}

// continuousIntegration reports whether a database-unavailable condition must
// fail the run rather than skip it.
func continuousIntegration() bool {
	return os.Getenv("CI") == "true"
}

// unavailable ends the test: a hard failure under CI, a skip naming the
// container command otherwise.
func unavailable(t *testing.T, err error) {
	t.Helper()
	if continuousIntegration() {
		t.Fatalf("CI=true and Postgres is unavailable at %s: %v", baseDatabaseURL(), err)
	}
	t.Skipf("Postgres unavailable at %s: %v\nstart one with: %s", baseDatabaseURL(), err,
		startContainerHint)
}

// openPool dials the supplied connection string with the supplied search path
// and verifies the connection before returning.
func openPool(ctx context.Context, connectionURL string, searchPath string) (*pgxpool.Pool, error) {
	poolConfig, err := pgxpool.ParseConfig(connectionURL)
	if err != nil {
		return nil, fmt.Errorf("parse connection string: %w", err)
	}
	if searchPath != "" {
		poolConfig.ConnConfig.RuntimeParams["search_path"] = searchPath
	}
	poolConfig.MaxConns = 8
	poolConfig.MinConns = 0

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

// closePoolWithDeadline closes a pool, giving up after the deadline so a leaked
// transaction cannot hang the suite.
func closePoolWithDeadline(pool *pgxpool.Pool, deadline time.Duration) error {
	if pool == nil {
		return nil
	}
	closed := make(chan struct{})
	go func() {
		pool.Close()
		close(closed)
	}()
	select {
	case <-closed:
		return nil
	case <-time.After(deadline):
		return errors.New("pool did not close within the deadline; a connection is still checked out")
	}
}
