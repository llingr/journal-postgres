// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/streams"

	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/tests/harness"
)

// TestAnAppendAfterTheOwnerDeletedTheHeadIsAConflict: a writer holding a stream whose head
// the owner deleted would leave a gap, which is refused as a conflict, so it reads the stream
// again and appends from the head that is left
func TestAnAppendAfterTheOwnerDeletedTheHeadIsAConflict(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	notes := newNotes(journal.NewStore(postgres.NewBackend(rig.AppPool())))
	streamID := uuid.New()

	stream, err := notes.CreateAll(ctx, streamID, []streams.DomainEvent{
		&noted{Text: "first"},
		&noted{Text: "second"},
		&noted{Text: "third"},
	})
	harness.RequireNoError(t, err, "create three events")
	_, err = rig.OwnerPool().Exec(ctx,
		`DELETE FROM events WHERE stream_id = $1 AND version = 3`, streamID.String())
	harness.RequireNoError(t, err, "delete the head as the owner")

	_, err = notes.Append(ctx, stream, &noted{Text: "past the gap"})
	if !errors.Is(err, streams.ErrVersionConflict) {
		t.Fatalf("append of version 4 after version 3 was deleted: got %v, want "+
			"ErrVersionConflict", err)
	}
	reread, err := notes.Get(ctx, streamID)
	harness.RequireNoError(t, err, "read again")
	appended, err := notes.Append(ctx, reread, &noted{Text: "the new third"})
	if err != nil || appended.Version() != 3 || appended.State() != 3 {
		t.Fatalf("append after reading again: %d events at version %d, %v; want 3 at "+
			"version 3", appended.State(), appended.Version(), err)
	}
}

// TestAStreamTheOwnerPunchedAHoleInIsNeverFolded: a stream missing a version the owner
// deleted from its middle fails the read rather than folding part of it
func TestAStreamTheOwnerPunchedAHoleInIsNeverFolded(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	notes := newNotes(journal.NewStore(postgres.NewBackend(rig.AppPool())))
	streamID := uuid.New()

	_, err := notes.CreateAll(ctx, streamID, []streams.DomainEvent{
		&noted{Text: "first"},
		&noted{Text: "second"},
		&noted{Text: "third"},
	})
	harness.RequireNoError(t, err, "create three events")
	_, err = rig.OwnerPool().Exec(ctx,
		`DELETE FROM events WHERE stream_id = $1 AND version = 2`, streamID.String())
	harness.RequireNoError(t, err, "delete version 2 as the owner")

	_, err = notes.Get(ctx, streamID)
	if !errors.Is(err, streams.ErrVersionMissing) {
		t.Fatalf("get of a stream missing version 2: got %v, want ErrVersionMissing", err)
	}
}
