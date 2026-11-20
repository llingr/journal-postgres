// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"reflect"
	"strings"
	"testing"
)

// TestATableNameMustBeALowercaseIdentifier: a name goes into the statements as it is, so one
// that could change their meaning, or that Postgres would fold to another table, stops the
// program where the store is wired.
func TestATableNameMustBeALowercaseIdentifier(t *testing.T) {
	refused := []string{
		"",
		"Order_Events",
		"order-events",
		"1events",
		"order events",
		"events; DROP TABLE events",
		strings.Repeat("e", 64),
	}
	for _, name := range refused {
		for optionName, option := range map[string]func(string) Option{
			"WithEventsTableName":       WithEventsTableName,
			"WithEventsOutboxTableName": WithEventsOutboxTableName,
		} {
			t.Run(optionName+" "+name, func(t *testing.T) {
				defer func() {
					if recover() == nil {
						t.Fatalf("%s(%q) did not panic", optionName, name)
					}
				}()
				option(name)
			})
		}
	}

	for _, name := range []string{"order_events", "_events", strings.Repeat("e", 63)} {
		WithEventsTableName(name)
		WithEventsOutboxTableName(name)
	}
}

// TestEveryStatementNamesTheStoresTables: no statement is left with a placeholder or a
// default table name once a store is given its own, and the defaults fill in as events and
// events_outbox.
func TestEveryStatementNamesTheStoresTables(t *testing.T) {
	custom := newStoreQueries("order_log", "order_log_pending")
	for name, statement := range queryStatements(custom) {
		if strings.Contains(statement, "{") {
			t.Errorf("%s keeps a placeholder: %s", name, statement)
		}
		if strings.Contains(statement, " events ") || strings.Contains(statement, "events_outbox") {
			t.Errorf("%s names a default table: %s", name, statement)
		}
		if !strings.Contains(statement, "order_log") {
			t.Errorf("%s names neither of the store's tables: %s", name, statement)
		}
	}

	defaults := newStoreQueries(DefaultEventsTableName, DefaultEventsOutboxTableName)
	if !strings.Contains(defaults.insertEvents, "INSERT INTO events ") ||
		!strings.Contains(defaults.insertOutbox, "INSERT INTO events_outbox ") {
		t.Fatalf("the default statements insert into %q and %q", defaults.insertEvents,
			defaults.insertOutbox)
	}
}

// queryStatements is each of the store's statements by its field name
func queryStatements(queries storeQueries) map[string]string {
	statements := make(map[string]string)
	value := reflect.ValueOf(queries)
	for index := range value.NumField() {
		statements[value.Type().Field(index).Name] = value.Field(index).String()
	}
	return statements
}
