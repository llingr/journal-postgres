// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"embed"
	"fmt"
	"strings"

	"github.com/llingr/llingr-pgx/queries"
)

// The placeholders queries/events.sql names its tables by, filled in for each store
const (
	eventsTablePlaceholder = "{events_table}"
	outboxTablePlaceholder = "{events_outbox_table}"
)

// The store's statements, by their name in queries/events.sql
const (
	readStreamQuery      = "read-stream"
	readStateQuery       = "read-state"
	saveStateQuery       = "save-state"
	streamHeadQuery      = "stream-head"
	insertEventsQuery    = "insert-events"
	insertOutboxQuery    = "insert-outbox"
	earliestOutboxQuery  = "earliest-outbox"
	outboxVersionsQuery  = "outbox-versions"
	outboxStreamsQuery   = "outbox-streams"
	deletePublishedQuery = "delete-published"
)

//go:embed queries/*.sql
var embeddedQueries embed.FS

var statements = loadStatements()

// loadStatements loads the embedded SQL and panics when a statement the store names is
// missing, so a mistake stops the program at startup rather than at its first call
func loadStatements() queries.Statements {
	loaded, err := queries.Load(embeddedQueries)
	if err != nil {
		panic(fmt.Errorf("load the event store's queries: %w", err))
	}
	names := []string{
		readStreamQuery,
		readStateQuery,
		saveStateQuery,
		streamHeadQuery,
		insertEventsQuery,
		insertOutboxQuery,
		earliestOutboxQuery,
		outboxVersionsQuery,
		outboxStreamsQuery,
		deletePublishedQuery,
	}
	for _, name := range names {
		if loaded.SQL(name) == "" {
			panic(fmt.Errorf("the event store's queries have no %q", name))
		}
	}
	return loaded
}

// storeQueries is one store's statements, its table names in place
type storeQueries struct {
	readStream      string
	readState       string
	saveState       string
	streamHead      string
	insertEvents    string
	insertOutbox    string
	earliestOutbox  string
	outboxVersions  string
	outboxStreams   string
	deletePublished string
}

// newStoreQueries fills the table names into every statement, once, when the store is built
func newStoreQueries(eventsTable string, outboxTable string) storeQueries {
	tables := strings.NewReplacer(eventsTablePlaceholder, eventsTable,
		outboxTablePlaceholder, outboxTable)
	named := func(name string) string {
		return tables.Replace(statements.SQL(name))
	}
	return storeQueries{
		readStream:      named(readStreamQuery),
		readState:       named(readStateQuery),
		saveState:       named(saveStateQuery),
		streamHead:      named(streamHeadQuery),
		insertEvents:    named(insertEventsQuery),
		insertOutbox:    named(insertOutboxQuery),
		earliestOutbox:  named(earliestOutboxQuery),
		outboxVersions:  named(outboxVersionsQuery),
		outboxStreams:   named(outboxStreamsQuery),
		deletePublished: named(deletePublishedQuery),
	}
}
