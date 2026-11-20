// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package postgres is the Postgres backend for journal: NewBackend over a pgx pool, passed to
// journal.NewStore. The migrations it needs are embedded in the schema package.
package postgres

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// ErrNoPool is returned when a store is built without somewhere to write.
var ErrNoPool = errors.New("no connection pool supplied")

// store holds the pool the app role connects with; the owner's DDL belongs to the host's
// migrations
type store struct {
	pool        *pgxpool.Pool
	eventsTable string
	outboxTable string
	queries     storeQueries
}

// NewBackend returns the persistence.Backend over Postgres, writing through the supplied pool,
// which should be opened as the app role, to the tables events and events_outbox unless the
// options name others. Panics on a nil pool, because that is a wiring mistake rather than a
// runtime condition.
func NewBackend(pool *pgxpool.Pool, options ...Option) persistence.Backend {
	if pool == nil {
		panic(ErrNoPool)
	}
	settings := storeSettings{
		eventsTable: DefaultEventsTableName,
		outboxTable: DefaultEventsOutboxTableName,
	}
	for _, option := range options {
		option(&settings)
	}
	return &store{
		pool:        pool,
		eventsTable: settings.eventsTable,
		outboxTable: settings.outboxTable,
		queries:     newStoreQueries(settings.eventsTable, settings.outboxTable),
	}
}

// Postgres errors the store maps to the events package's
const (
	uniqueViolation       = "23505"
	checkViolation        = "23514"
	insufficientPrivilege = "42501"

	// the constraint the gap trigger names, refusing a version past the stream's head
	versionsContiguous = "events_versions_contiguous"
)

// storeError adds the typed error a caller acts on to a database error, keeping the driver's
// error for its detail. A unique violation on either table is a version another writer took,
// as their primary keys are their only unique constraints, whatever the host named them, and
// the gap trigger's refusal is a version past the head; either way the caller reads again.
func (st *store) storeError(err error) error {
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		return err
	}
	switch {
	case postgresError.Code == uniqueViolation && (postgresError.TableName == st.eventsTable ||
		postgresError.TableName == st.outboxTable):
		return fmt.Errorf("%w: %w", streams.ErrVersionConflict, err)
	case postgresError.Code == checkViolation &&
		postgresError.ConstraintName == versionsContiguous:
		return fmt.Errorf("%w: %w", streams.ErrVersionConflict, err)
	case postgresError.Code == insufficientPrivilege:
		return fmt.Errorf("%w: %w", streams.ErrNotPermitted, err)
	}
	return err
}

// uuidValue wraps a UUID for pgx.
func uuidValue(value uuid.UUID) pgtype.UUID {
	return pgtype.UUID{
		Bytes: value,
		Valid: true,
	}
}
