// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/journaltest"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/persistence/backendtest"
	"github.com/llingr/journal/streams"

	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/tests/harness"
)

// TestEverythingHoldsUnderOtherTableNames: with the tables and their primary keys named as a
// host's own migrations might, the contract and the library suite still hold, conflicts
// included, as the store finds them by table rather than by constraint name, and sweeps
// included, as they read the store's own outbox
func TestEverythingHoldsUnderOtherTableNames(t *testing.T) {
	newBackend := func(t *testing.T) persistence.Backend {
		rig := harness.New(t)
		renameTables(t, rig, "order_events", "order_events_outbox")
		return postgres.NewBackend(rig.AppPool(),
			postgres.WithEventsTableName("order_events"),
			postgres.WithEventsOutboxTableName("order_events_outbox"))
	}
	t.Run("Contract", func(t *testing.T) {
		backendtest.RunContract(t, newBackend)
	})
	t.Run("Library", func(t *testing.T) {
		journaltest.RunSuite(t, newBackend)
	})
}

// TestTwoJournalsInOneSchemaStayApart: two stores over their own tables keep separate streams
// and outboxes, so one stream id in both is two streams, and each publishes only its own.
func TestTwoJournalsInOneSchemaStayApart(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	renameTables(t, rig, "order_events", "order_events_outbox")
	for _, statement := range []string{
		`CREATE TABLE payment_events (LIKE order_events INCLUDING ALL)`,
		`CREATE TABLE payment_events_outbox (LIKE order_events_outbox INCLUDING ALL)`,
		`GRANT SELECT, INSERT ON payment_events TO ` + harness.AppUsername,
		`GRANT SELECT, INSERT, DELETE ON payment_events_outbox TO ` + harness.AppUsername,
	} {
		_, err := rig.OwnerPool().Exec(ctx, statement)
		harness.RequireNoError(t, err, statement)
	}

	orderPublisher, paymentPublisher := &recordingPublisher{}, &recordingPublisher{}
	orders := newNotes(journal.NewStore(postgres.NewBackend(rig.AppPool(),
		postgres.WithEventsTableName("order_events"),
		postgres.WithEventsOutboxTableName("order_events_outbox")),
		journal.WithPublisher(orderPublisher)))
	payments := newNotes(journal.NewStore(postgres.NewBackend(rig.AppPool(),
		postgres.WithEventsTableName("payment_events"),
		postgres.WithEventsOutboxTableName("payment_events_outbox")),
		journal.WithPublisher(paymentPublisher)))
	streamID := uuid.New()

	_, err := orders.Create(ctx, streamID, &noted{Text: "an order"})
	harness.RequireNoError(t, err, "create the order stream")
	_, err = payments.Create(ctx, streamID, &noted{Text: "a payment"})
	harness.RequireNoError(t, err, "create the payment stream with the same id")
	_, err = orders.Create(ctx, streamID, &noted{Text: "the order again"})
	if !errors.Is(err, streams.ErrStreamExists) {
		t.Fatalf("a second create in the order journal: got %v, want ErrStreamExists", err)
	}

	for name, published := range map[string]*recordingPublisher{
		"orders":   orderPublisher,
		"payments": paymentPublisher,
	} {
		if versions := published.versions(); len(versions) != 1 || versions[0] != 1 {
			t.Errorf("%s published %v, want its own version 1 alone", name, versions)
		}
	}
}

// renameTables gives the migrated tables and their primary keys other names, as a host's own
// migrations might, leaving the grants as the migrations made them
func renameTables(t *testing.T, rig *harness.Harness, eventsTable string, outboxTable string) {
	t.Helper()
	ctx := context.Background()
	for _, statement := range []string{
		`ALTER TABLE events RENAME TO ` + eventsTable,
		`ALTER TABLE ` + eventsTable + ` RENAME CONSTRAINT pk_events TO ` + eventsTable + `_key`,
		`ALTER TABLE events_outbox RENAME TO ` + outboxTable,
		`ALTER TABLE ` + outboxTable + ` RENAME CONSTRAINT pk_events_outbox TO ` +
			outboxTable + `_key`,
	} {
		_, err := rig.OwnerPool().Exec(ctx, statement)
		harness.RequireNoError(t, err, statement)
	}
}

// newNotes is a repository of noted events over the store, counting them
func newNotes(eventStore *journal.Store) *streams.Repository[int] {
	return journal.NewRepository(eventStore, journal.WithTransitions(
		journal.Transition(func(count int, _ *noted) (int, error) {
			return count + 1, nil
		}),
	))
}
