// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"fmt"
	"strings"

	"github.com/llingr/llingr-pgx/roles"
)

// The table names the embedded migrations create, and a store uses without options
const (
	DefaultEventsTableName       = "events"
	DefaultEventsOutboxTableName = "events_outbox"
)

// Option configures a store
type Option func(settings *storeSettings)

type storeSettings struct {
	eventsTable string
	outboxTable string
}

// WithEventsTableName keeps events in the named table rather than events, for a host whose own
// migrations create it with the embedded schema's columns. Panics on a name that is not a
// lowercase SQL identifier, as that is a wiring mistake.
func WithEventsTableName(name string) Option {
	requireTableName("WithEventsTableName", name)
	return func(settings *storeSettings) {
		settings.eventsTable = name
	}
}

// WithEventsOutboxTableName keeps the outbox in the named table rather than events_outbox, as
// WithEventsTableName does for the events
func WithEventsOutboxTableName(name string) Option {
	requireTableName("WithEventsOutboxTableName", name)
	return func(settings *storeSettings) {
		settings.outboxTable = name
	}
}

// requireTableName panics unless name is a lowercase SQL identifier: it goes into the
// statements as it is, and Postgres folds an unquoted name to lowercase, so capitals would
// name another table
func requireTableName(option string, name string) {
	if !roles.IsPlainSQLIdentifier(name) || name != strings.ToLower(name) {
		panic(fmt.Sprintf("postgres.%s(%q): want a lowercase SQL identifier of letters, digits "+
			"and underscores, at most %d bytes", option, name, roles.MaxIdentifierBytes))
	}
}
