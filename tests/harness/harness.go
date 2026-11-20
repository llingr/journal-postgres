// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// poolCloseDeadline bounds pool shutdown so one checked-out connection cannot
// hang the whole suite.
const poolCloseDeadline = 10 * time.Second

// Harness owns one isolated Postgres schema, migrated, with the owner and app
// pools. Every privilege assertion connects as the role it is about.
type Harness struct {
	t          *testing.T
	SchemaName string

	ownerPool *pgxpool.Pool
	appPool   *pgxpool.Pool
}

// New prepares an isolated schema with the migrations applied and registers
// the cleanup that drops it. It skips locally and fails under CI when Postgres
// is unreachable.
func New(t *testing.T) *Harness {
	t.Helper()
	ctx := context.Background()

	if err := bootstrapCluster(ctx); err != nil {
		unavailable(t, err)
	}

	harness := &Harness{
		t:          t,
		SchemaName: randomSchemaName(),
	}
	t.Cleanup(harness.close)

	administrativePool, err := openPool(ctx, testDatabaseURL(), "")
	if err != nil {
		unavailable(t, err)
	}
	defer func() {
		_ = closePoolWithDeadline(administrativePool, poolCloseDeadline)
	}()

	createSchema := fmt.Sprintf(`CREATE SCHEMA %s AUTHORIZATION %s`,
		quoteIdentifier(harness.SchemaName), quoteIdentifier(OwnerUsername))
	if _, err = administrativePool.Exec(ctx, createSchema); err != nil {
		t.Fatalf("create schema %s: %v", harness.SchemaName, err)
	}
	grantUsage := fmt.Sprintf(`GRANT USAGE ON SCHEMA %s TO %s`,
		quoteIdentifier(harness.SchemaName), quoteIdentifier(AppUsername))
	if _, err = administrativePool.Exec(ctx, grantUsage); err != nil {
		t.Fatalf("grant usage on schema %s: %v", harness.SchemaName, err)
	}

	harness.ownerPool = harness.mustOpen(roleDatabaseURL(OwnerUsername))
	if err = applyMigrations(ctx, harness.ownerPool); err != nil {
		t.Fatalf("apply migrations into schema %s: %v", harness.SchemaName, err)
	}
	return harness
}

// Migrate runs the up-migrations again against the same schema, which is how
// the no-op property of a second run is asserted.
func (harness *Harness) Migrate() error {
	return applyMigrations(context.Background(), harness.ownerPool)
}

// OwnerPool runs DDL and the deletes. It must never be used for a privilege assertion.
func (harness *Harness) OwnerPool() *pgxpool.Pool {
	return harness.ownerPool
}

// AppPool appends events and outbox rows.
func (harness *Harness) AppPool() *pgxpool.Pool {
	if harness.appPool == nil {
		harness.appPool = harness.mustOpen(roleDatabaseURL(AppUsername))
	}
	return harness.appPool
}

// OpenExtraAppPool returns an additional app pool with its own connections, for
// tests that need two sessions racing.
func (harness *Harness) OpenExtraAppPool() *pgxpool.Pool {
	pool := harness.mustOpen(roleDatabaseURL(AppUsername))
	harness.t.Cleanup(func() {
		if err := closePoolWithDeadline(pool, poolCloseDeadline); err != nil {
			harness.t.Errorf("close extra app pool: %v", err)
		}
	})
	return pool
}

// OpenTracedAppPool returns an app pool whose statements the tracer observes, for tests that
// count round trips
func (harness *Harness) OpenTracedAppPool(tracer pgx.QueryTracer) *pgxpool.Pool {
	harness.t.Helper()
	poolConfig, err := pgxpool.ParseConfig(roleDatabaseURL(AppUsername))
	if err != nil {
		harness.t.Fatalf("parse the app connection string: %v", err)
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = harness.SchemaName
	poolConfig.ConnConfig.Tracer = tracer
	poolConfig.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		harness.t.Fatalf("open a traced app pool: %v", err)
	}
	harness.t.Cleanup(func() {
		_ = closePoolWithDeadline(pool, poolCloseDeadline)
	})
	return pool
}

func (harness *Harness) mustOpen(connectionURL string) *pgxpool.Pool {
	harness.t.Helper()
	pool, err := openPool(context.Background(), connectionURL, harness.SchemaName)
	if err != nil {
		harness.t.Fatalf("open pool on schema %s: %v", harness.SchemaName, err)
	}
	return pool
}

// close releases every pool and drops the schema. Role pools go first so the
// drop meets no session holding an object in the schema.
func (harness *Harness) close() {
	if err := closePoolWithDeadline(harness.appPool, poolCloseDeadline); err != nil {
		harness.t.Errorf("close the app pool on schema %s: %v", harness.SchemaName, err)
	}
	if err := closePoolWithDeadline(harness.ownerPool, poolCloseDeadline); err != nil {
		harness.t.Errorf("close owner pool on schema %s: %v", harness.SchemaName, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), poolCloseDeadline)
	defer cancel()

	administrativePool, err := openPool(ctx, testDatabaseURL(), "")
	if err != nil {
		harness.t.Errorf("connect to drop schema %s: %v", harness.SchemaName, err)
		return
	}
	defer func() {
		_ = closePoolWithDeadline(administrativePool, poolCloseDeadline)
	}()

	dropSchema := fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, quoteIdentifier(harness.SchemaName))
	if _, err = administrativePool.Exec(ctx, dropSchema); err != nil {
		harness.t.Errorf("drop schema %s: %v", harness.SchemaName, err)
	}
}

func randomSchemaName() string {
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		panic(fmt.Sprintf("harness: random schema name: %v", err))
	}
	return "event_store_test_" + hex.EncodeToString(suffix)
}
