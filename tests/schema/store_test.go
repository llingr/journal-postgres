// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"

	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/tests/harness"
)

// TestAppendWithoutTheInsertPrivilegeIsNotPermitted: a store over the wrong role reports the
// privilege, not a driver error a caller has to pick apart.
func TestAppendWithoutTheInsertPrivilegeIsNotPermitted(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	_, err := rig.OwnerPool().Exec(ctx, `REVOKE INSERT ON events FROM `+harness.AppUsername)
	harness.RequireNoError(t, err, "revoke the app's insert")
	store := postgres.NewBackend(rig.AppPool())
	streamID := uuid.New()

	_, err = store.Append(ctx, appendRequest(streamID, 1))
	if !errors.Is(err, streams.ErrNotPermitted) {
		t.Fatalf("append without the insert privilege: got %v, want ErrNotPermitted", err)
	}
}

// TestTheEventsAndTheirOutboxRowsCommitTogether: an append's events and outbox rows stand or
// fall as one, whichever of them the database refuses.
func TestTheEventsAndTheirOutboxRowsCommitTogether(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	store := postgres.NewBackend(rig.AppPool())
	streamID := uuid.New()
	appendStoreVersions(t, store, streamID, 1)
	requireRows(t, rig, streamID, 1, 1)

	// an outbox row already holds version 2, so the events go with the refused outbox insert
	_, err := rig.OwnerPool().Exec(ctx, `INSERT INTO events_outbox
        (stream_id, version, event_append_ts) VALUES ($1, 2, now())`, streamID.String())
	harness.RequireNoError(t, err, "plant an outbox row for version 2")
	_, err = store.Append(ctx, appendRequest(streamID, 2))
	if !errors.Is(err, streams.ErrVersionConflict) {
		t.Fatalf("append over a planted outbox row: got %v, want ErrVersionConflict", err)
	}
	requireRows(t, rig, streamID, 1, 2)

	// the events repeat a version, so their outbox rows go with them
	_, err = rig.OwnerPool().Exec(ctx, `DELETE FROM events_outbox WHERE stream_id = $1`,
		streamID.String())
	harness.RequireNoError(t, err, "clear the outbox")
	_, err = store.Append(ctx, appendRequest(streamID, 1))
	if !errors.Is(err, streams.ErrVersionConflict) {
		t.Fatalf("append of a repeated version: got %v, want ErrVersionConflict", err)
	}
	requireRows(t, rig, streamID, 1, 0)
}

// TestAnAppendIsOneRoundTrip: the events, their outbox rows and the earliest unpublished
// version go as one batch, with no BEGIN or COMMIT of their own.
func TestAnAppendIsOneRoundTrip(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	tracer := &exchangeCounter{}
	store := postgres.NewBackend(rig.OpenTracedAppPool(tracer))
	streamID := uuid.New()

	appendStoreVersions(t, store, streamID, 1) // warms the connection
	tracer.exchanges.Store(0)
	_, err := store.Append(ctx, appendRequest(streamID, 2, 3))
	harness.RequireNoError(t, err, "append versions 2 and 3")
	if exchanges := tracer.exchanges.Load(); exchanges != 1 {
		t.Fatalf("an append took %d round trips, want 1", exchanges)
	}
	requireRows(t, rig, streamID, 3, 3)
}

// TestManyEventsAreOneInsertPerTable: an append of 25 events sends one insert into events and
// one into the outbox, with the read of earlier unpublished versions beside them, whatever the
// number of events.
func TestManyEventsAreOneInsertPerTable(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	tracer := &exchangeCounter{}
	store := postgres.NewBackend(rig.OpenTracedAppPool(tracer))
	streamID := uuid.New()

	versions := make([]int32, 25)
	for index := range versions {
		versions[index] = int32(index + 1)
	}
	_, err := store.Append(ctx, appendRequest(streamID, versions...))
	harness.RequireNoError(t, err, "append 25 events")
	if exchanges, statements := tracer.exchanges.Load(), tracer.statements.Load(); exchanges != 1 ||
		statements != 3 {
		t.Fatalf("an append of 25 events took %d round trips and %d statements, want 1 and 3",
			exchanges, statements)
	}
	requireRows(t, rig, streamID, 25, 25)

	read, err := store.QueryStream(ctx, streamID, 1, math.MaxInt32)
	harness.RequireNoError(t, err, "read the 25 events back")
	for index, event := range read {
		if event.Version != int32(index+1) ||
			string(event.EventData) != fmt.Sprintf(`{"version":%d}`, index+1) {
			t.Fatalf("event %d read back as version %d with %q", index, event.Version,
				event.EventData)
		}
	}
}

// TestQueryStreamRefusesAStreamWithAHole: a read never returns a stream with a version missing
// part way along, so a fold cannot build state from part of a stream.
func TestQueryStreamRefusesAStreamWithAHole(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	store := postgres.NewBackend(rig.AppPool())
	streamID := uuid.New()
	appendStoreVersions(t, store, streamID, 1, 2, 3, 4)

	_, err := rig.OwnerPool().Exec(ctx,
		`DELETE FROM events WHERE stream_id = $1 AND version = 3`, streamID.String())
	harness.RequireNoError(t, err, "delete version 3")

	for _, fromVersion := range []int32{1, 2} {
		_, err = store.QueryStream(ctx, streamID, fromVersion, math.MaxInt32)
		if !errors.Is(err, streams.ErrVersionMissing) || !strings.Contains(err.Error(), "version 3") {
			t.Fatalf("read from version %d of a stream without version 3: got %v, "+
				"want ErrVersionMissing naming version 3", fromVersion, err)
		}
	}
}

// TestQueryStreamAfterATrimNamesTheMissingVersion: a read from a version that is gone says
// which one, and a read from the first version kept returns the rest.
func TestQueryStreamAfterATrimNamesTheMissingVersion(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	store := postgres.NewBackend(rig.AppPool())
	streamID := uuid.New()
	appendStoreVersions(t, store, streamID, 1, 2, 3, 4)

	_, err := rig.OwnerPool().Exec(ctx,
		`DELETE FROM events WHERE stream_id = $1 AND version <= 2`, streamID.String())
	harness.RequireNoError(t, err, "trim versions 1 and 2")

	for fromVersion, missing := range map[int32]string{1: "version 1", 2: "version 2"} {
		_, err = store.QueryStream(ctx, streamID, fromVersion, math.MaxInt32)
		if !errors.Is(err, streams.ErrVersionMissing) || !strings.Contains(err.Error(), missing) {
			t.Fatalf("read from version %d after the trim: got %v, want ErrVersionMissing "+
				"naming %s", fromVersion, err, missing)
		}
	}

	read, err := store.QueryStream(ctx, streamID, 3, math.MaxInt32)
	harness.RequireNoError(t, err, "read from the first version kept")
	if len(read) != 2 || read[0].Version != 3 || read[1].Version != 4 {
		t.Fatalf("read from version 3 after the trim returned %d events, want 3 and 4", len(read))
	}
}

// storeEvent is one appendable event of the store's own type.
func storeEvent(streamID uuid.UUID, version int32) persistence.PersistedEvent {
	return persistence.PersistedEvent{
		StreamID:        streamID,
		Version:         version,
		EventType:       "OrderPlaced",
		EventData:       []byte(fmt.Sprintf(`{"version":%d}`, version)),
		DataFormat:      codec.JSON,
		DataCompression: compress.None,
		DataEncryption:  encrypt.None,
		EventAppendTs:   harness.DefaultAppendTs,
	}
}

// appendRequest is one stream's versions, published under the stream id
func appendRequest(streamID uuid.UUID, versions ...int32) persistence.AppendRequest {
	request := persistence.AppendRequest{
		StreamID: streamID,
		AppendTs: harness.DefaultAppendTs,
		Publish:  true,
	}
	for _, version := range versions {
		request.Events = append(request.Events, storeEvent(streamID, version))
	}
	return request
}

// appendStoreVersions appends one event per version through the store.
func appendStoreVersions(t *testing.T, store persistence.Backend, streamID uuid.UUID,
	versions ...int32) {
	t.Helper()
	_, err := store.Append(context.Background(), appendRequest(streamID, versions...))
	harness.RequireNoError(t, err, fmt.Sprintf("append versions %v", versions))
}

// requireRows fails unless the stream has the rows wanted in the events table and the outbox
func requireRows(t *testing.T, rig *harness.Harness, streamID uuid.UUID, wantEvents int,
	wantPending int) {
	t.Helper()
	ctx := context.Background()

	wants := []struct {
		table string
		rows  int
	}{
		{table: "events", rows: wantEvents},
		{table: "events_outbox", rows: wantPending},
	}
	for _, want := range wants {
		var count int
		err := rig.OwnerPool().QueryRow(ctx,
			`SELECT count(*) FROM `+want.table+` WHERE stream_id = $1`,
			streamID.String()).Scan(&count)
		harness.RequireNoError(t, err, "count rows in "+want.table)
		if count != want.rows {
			t.Errorf("%s holds %d rows for the stream, want %d", want.table, count, want.rows)
		}
	}
}

// exchangeCounter counts the round trips a connection makes, a statement or a batch each, and
// the statements inside batches
type exchangeCounter struct {
	exchanges  atomic.Int64
	statements atomic.Int64
}

func (c *exchangeCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn,
	_ pgx.TraceQueryStartData) context.Context {
	c.exchanges.Add(1)
	return ctx
}

func (c *exchangeCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {
}

func (c *exchangeCounter) TraceBatchStart(ctx context.Context, _ *pgx.Conn,
	_ pgx.TraceBatchStartData) context.Context {
	c.exchanges.Add(1)
	return ctx
}

func (c *exchangeCounter) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {
	c.statements.Add(1)
}

func (c *exchangeCounter) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {
}
