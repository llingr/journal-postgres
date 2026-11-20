// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// SQLSTATE codes asserted by the suite.
const (
	SQLStateCheckViolation            = "23514"
	SQLStateInvalidTextRepresentation = "22P02"
	SQLStateNotNullViolation          = "23502"
	SQLStateUniqueViolation           = "23505"
	SQLStateForeignKeyViolation       = "23503"
	SQLStateInsufficientPrivilege     = "42501"
	SQLStateDuplicateObject           = "42710"
	SQLStateDuplicateDatabase         = "42P04"
)

// Constraint names raised by the schema.
const (
	ConstraintEventsVersion           = "chk_events_version"
	ConstraintEventsPrimaryKey        = "pk_events"
	ConstraintOutboxPrimaryKey        = "pk_events_outbox"
	ConstraintEventsEncryptionPairing = "chk_events_data_encryption_none"
	ConstraintEventsContiguous        = "events_versions_contiguous"
)

// AsPostgresError extracts the server error, reporting whether the error came
// from Postgres at all.
func AsPostgresError(err error) (*pgconn.PgError, bool) {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		return postgresError, true
	}
	return nil, false
}

// RequireSQLState fails unless err is a Postgres error carrying the wanted
// SQLSTATE. An empty wantConstraint leaves the constraint name unchecked.
func RequireSQLState(t *testing.T, err error, wantSQLState string, wantConstraint string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected SQLSTATE %s, got no error", wantSQLState)
	}
	postgresError, isPostgres := AsPostgresError(err)
	if !isPostgres {
		t.Fatalf("expected SQLSTATE %s, got a non-Postgres error: %v", wantSQLState, err)
	}
	if postgresError.Code != wantSQLState {
		t.Fatalf("expected SQLSTATE %s, got %s (constraint %q): %s",
			wantSQLState, postgresError.Code, postgresError.ConstraintName, postgresError.Message)
	}
	if wantConstraint != "" && postgresError.ConstraintName != wantConstraint {
		t.Fatalf("expected constraint %q, got %q (SQLSTATE %s): %s",
			wantConstraint, postgresError.ConstraintName, postgresError.Code, postgresError.Message)
	}
}

// RequireNoError fails with the server message when the statement was expected
// to succeed.
func RequireNoError(t *testing.T, err error, context string) {
	t.Helper()
	if err == nil {
		return
	}
	if postgresError, isPostgres := AsPostgresError(err); isPostgres {
		t.Fatalf("%s: SQLSTATE %s constraint %q: %s",
			context, postgresError.Code, postgresError.ConstraintName, postgresError.Message)
	}
	t.Fatalf("%s: %v", context, err)
}

func isDuplicateObject(err error) bool {
	postgresError, isPostgres := AsPostgresError(err)
	if !isPostgres {
		return false
	}
	return postgresError.Code == SQLStateDuplicateObject ||
		postgresError.Code == SQLStateDuplicateDatabase
}
