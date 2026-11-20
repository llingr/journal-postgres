// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
)

// bootstrapOnce guards the cluster-wide preparation: the four login roles and
// the database holding every per-test schema.
var (
	bootstrapOnce   sync.Once
	bootstrapResult error
)

// allRoleUsernames lists the two roles the schema knows.
var allRoleUsernames = []string{OwnerUsername, AppUsername}

// bootstrapCluster creates the two roles and the test database if absent. It is
// idempotent, so a second test binary against the same cluster is a no-op.
func bootstrapCluster(ctx context.Context) error {
	bootstrapOnce.Do(func() {
		bootstrapResult = runBootstrap(ctx)
	})
	return bootstrapResult
}

func runBootstrap(ctx context.Context) error {
	superuserConnection, err := pgx.Connect(ctx, baseDatabaseURL())
	if err != nil {
		return fmt.Errorf("connect as superuser: %w", err)
	}
	defer func() {
		_ = superuserConnection.Close(ctx)
	}()

	for _, username := range allRoleUsernames {
		if err = createRoleIfAbsent(ctx, superuserConnection, username); err != nil {
			return err
		}
	}
	if err = createDatabaseIfAbsent(ctx, superuserConnection, TestDatabaseName); err != nil {
		return err
	}

	testDatabaseConnection, err := pgx.Connect(ctx, testDatabaseURL())
	if err != nil {
		return fmt.Errorf("connect to %s: %w", TestDatabaseName, err)
	}
	defer func() {
		_ = testDatabaseConnection.Close(ctx)
	}()

	// The owner creates a schema per test; the app only ever uses one.
	grants := []string{
		fmt.Sprintf(`GRANT CREATE, CONNECT ON DATABASE %s TO %s`, quoteIdentifier(TestDatabaseName),
			quoteIdentifier(OwnerUsername)),
		fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s`, quoteIdentifier(TestDatabaseName),
			quoteIdentifier(AppUsername)),
		// No role may fall back to public; every object under test is reached
		// through the per-test schema on the search path.
		`REVOKE ALL ON SCHEMA public FROM PUBLIC`,
	}
	for _, statement := range grants {
		if _, err = testDatabaseConnection.Exec(ctx, statement); err != nil {
			return fmt.Errorf("bootstrap grant %q: %w", statement, err)
		}
	}
	return nil
}

func createRoleIfAbsent(ctx context.Context, connection *pgx.Conn, username string) error {
	var exists bool
	err := connection.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`,
		username).Scan(&exists)
	if err != nil {
		return fmt.Errorf("look up role %s: %w", username, err)
	}
	if exists {
		return nil
	}
	statement := fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD %s`, quoteIdentifier(username),
		quoteLiteral(RolePassword))
	if _, err = connection.Exec(ctx, statement); err != nil && !isDuplicateObject(err) {
		return fmt.Errorf("create role %s: %w", username, err)
	}
	return nil
}

func createDatabaseIfAbsent(ctx context.Context, connection *pgx.Conn, databaseName string) error {
	var exists bool
	err := connection.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, databaseName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("look up database %s: %w", databaseName, err)
	}
	if exists {
		return nil
	}
	statement := fmt.Sprintf(`CREATE DATABASE %s`, quoteIdentifier(databaseName))
	if _, err = connection.Exec(ctx, statement); err != nil && !isDuplicateObject(err) {
		return fmt.Errorf("create database %s: %w", databaseName, err)
	}
	return nil
}

// testDatabaseURL rewrites the bootstrap URL to name the test database and
// leaves the superuser credentials in place.
func testDatabaseURL() string {
	return replaceDatabaseAndUser(baseDatabaseURL(), TestDatabaseName, "", "")
}

// roleDatabaseURL rewrites the bootstrap URL for one of the four test roles.
func roleDatabaseURL(username string) string {
	return replaceDatabaseAndUser(baseDatabaseURL(), TestDatabaseName, username, RolePassword)
}

func replaceDatabaseAndUser(connectionURL string, databaseName string, username string,
	password string) string {
	parsed, err := url.Parse(connectionURL)
	if err != nil {
		return connectionURL
	}
	parsed.Path = "/" + databaseName
	if username != "" {
		parsed.User = url.UserPassword(username, password)
	}
	return parsed.String()
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func quoteLiteral(literal string) string {
	return `'` + strings.ReplaceAll(literal, `'`, `''`) + `'`
}
