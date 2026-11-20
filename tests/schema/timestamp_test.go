// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal-postgres/tests/harness"
)

// statementDelay separates the two writes so a per-statement clock would show.
const statementDelay = 50 * time.Millisecond

// writeBothTables inserts one event and one outbox row in a single
// transaction, passing the same Go value to every statement, which is how
// the append takes its timestamp.
func writeBothTables(t *testing.T, rig *harness.Harness, appendTs time.Time) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	streamID := uuid.New()

	transaction, err := rig.AppPool().Begin(ctx)
	harness.RequireNoError(t, err, "begin the transaction")
	defer func() {
		_ = transaction.Rollback(ctx)
	}()

	_, err = transaction.Exec(ctx, `
		INSERT INTO events (stream_id, version, data_format, data_compression,
		                    data_encryption, event_append_ts, event_data, event_type,
		                    data_encryption_kid, metadata, partition_key)
		VALUES ($1, 1, 'json', 'none', 'none', $2, '\x41', 'OrderPlaced', '', '', '')`,
		streamID.String(), appendTs)
	harness.RequireNoError(t, err, "insert the event")

	time.Sleep(statementDelay)

	_, err = transaction.Exec(ctx, `
		INSERT INTO events_outbox (stream_id, version, event_append_ts)
		VALUES ($1, 1, $2)`, streamID.String(), appendTs)
	harness.RequireNoError(t, err, "insert the outbox row")

	harness.RequireNoError(t, transaction.Commit(ctx), "commit the transaction")
	return streamID
}

// distinctTimestamps returns every distinct value the two tables hold for one
// stream.
func distinctTimestamps(t *testing.T, rig *harness.Harness, streamID uuid.UUID) []time.Time {
	t.Helper()
	rows, err := rig.AppPool().Query(context.Background(), `
		SELECT DISTINCT written_ts FROM (
			SELECT event_append_ts AS written_ts FROM events                 WHERE stream_id = $1
			UNION ALL
			SELECT event_append_ts              FROM events_outbox WHERE stream_id = $1
		) AS written_timestamps
		ORDER BY written_ts`, streamID.String())
	harness.RequireNoError(t, err, "read the distinct timestamps")
	defer rows.Close()

	var found []time.Time
	for rows.Next() {
		var written time.Time
		harness.RequireNoError(t, rows.Scan(&written), "scan a timestamp")
		found = append(found, written)
	}
	harness.RequireNoError(t, rows.Err(), "iterate the timestamps")
	return found
}

// TestOneBatchTimestampLandsInBothTables covers contract I1.
func TestOneBatchTimestampLandsInBothTables(t *testing.T) {
	rig := harness.New(t)
	appendTs := time.Now().UTC().Truncate(time.Microsecond)

	streamID := writeBothTables(t, rig, appendTs)

	found := distinctTimestamps(t, rig, streamID)
	if len(found) != 1 {
		t.Fatalf("the two tables hold %d distinct values %v, want 1", len(found), found)
	}
	if !found[0].Equal(appendTs) {
		t.Fatalf("the two tables hold %s, want the value sent, %s", found[0], appendTs)
	}
}

// TestClientAppendTimestampIsStoredAsGiven covers contract I2.
func TestClientAppendTimestampIsStoredAsGiven(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	supplied := []struct {
		name     string
		appendTs time.Time
	}{
		{"backdated_by_a_decade", time.Date(2016, time.January, 2, 3, 4, 5, 678901000, time.UTC)},
		{"before_the_epoch", time.Date(1969, time.July, 20, 20, 17, 40, 0, time.UTC)},
		{"far_future", time.Date(2999, time.December, 31, 23, 59, 59, 999999000, time.UTC)},
	}

	for _, attempt := range supplied {
		t.Run(attempt.name, func(t *testing.T) {
			streamID := uuid.New()
			event := harness.NewEvent(streamID, 1)
			event.EventAppendTs = attempt.appendTs
			harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), event),
				"append with a client timestamp")

			stored, err := harness.ReadStream(ctx, rig.AppPool(), streamID)
			harness.RequireNoError(t, err, "read the event back")
			if !stored[0].EventAppendTs.Equal(attempt.appendTs) {
				t.Fatalf("stored %s, want %s", stored[0].EventAppendTs, attempt.appendTs)
			}
		})
	}
}

// TestUntruncatedTimestampDoesNotRoundTrip covers contract I3, first fact. A
// value carrying nanoseconds comes back as something else, so anything
// comparing the caller's variable against the record disagrees.
func TestUntruncatedTimestampDoesNotRoundTrip(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := uuid.New()

	untruncated := time.Date(2026, time.September, 8, 12, 0, 0, 750, time.UTC)
	if untruncated.Equal(untruncated.Truncate(time.Microsecond)) {
		t.Fatal("the chosen value carries no sub-microsecond remainder")
	}

	event := harness.NewEvent(streamID, 1)
	event.EventAppendTs = untruncated
	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), event),
		"append an untruncated value")

	stored, err := harness.ReadStream(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the event back")

	if stored[0].EventAppendTs.Equal(untruncated) {
		t.Fatalf("the sub-microsecond remainder survived the round trip: stored %s",
			stored[0].EventAppendTs.Format(time.RFC3339Nano))
	}
}

// TestTruncatedTimestampRoundTrips covers contract I3, second fact, which is
// what R7's Truncate buys.
func TestTruncatedTimestampRoundTrips(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := uuid.New()

	batchTs := time.Now().UTC().Truncate(time.Microsecond)

	events := make([]harness.Event, 0, 3)
	for version := int32(1); version <= 3; version++ {
		event := harness.NewEvent(streamID, version)
		event.EventAppendTs = batchTs
		events = append(events, event)
	}
	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), events...), "append the batch")

	stored, err := harness.ReadStream(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the stream back")
	if len(stored) != 3 {
		t.Fatalf("stream holds %d events, want 3", len(stored))
	}

	for index, event := range stored {
		if !event.EventAppendTs.Equal(batchTs) {
			t.Fatalf("event %d stored %s, want the value sent, %s",
				index+1, event.EventAppendTs.Format(time.RFC3339Nano), batchTs.Format(time.RFC3339Nano))
		}
	}
}

// TestBackdatedAppendTimestampBreaksOutboxStaleness records that a stream's
// append timestamps need not increase with version, so the sweeps' age filter
// reads a value the caller controls.
func TestBackdatedAppendTimestampBreaksOutboxStaleness(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := uuid.New()

	first := harness.NewEvent(streamID, 1)
	first.EventAppendTs = harness.DefaultAppendTs
	second := harness.NewEvent(streamID, 2)
	second.EventAppendTs = harness.DefaultAppendTs.Add(-30 * 24 * time.Hour)

	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), first, second),
		"append a backdated successor")
	harness.RequireNoError(t,
		harness.InsertOutboxRow(ctx, rig.AppPool(), streamID, 1, first.EventAppendTs),
		"queue version 1")
	harness.RequireNoError(t,
		harness.InsertOutboxRow(ctx, rig.AppPool(), streamID, 2, second.EventAppendTs),
		"queue version 2")

	rows, err := rig.AppPool().Query(ctx, `
		SELECT version FROM events_outbox WHERE stream_id = $1 ORDER BY event_append_ts`,
		streamID.String())
	harness.RequireNoError(t, err, "read the outbox in staleness order")
	defer rows.Close()

	var order []int32
	for rows.Next() {
		var version int32
		harness.RequireNoError(t, rows.Scan(&version), "scan a version")
		order = append(order, version)
	}
	harness.RequireNoError(t, rows.Err(), "iterate the outbox")

	if len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Fatalf("staleness order is %v, want [2 1]: a backdated event sorts ahead of its "+
			"predecessor", order)
	}
}
