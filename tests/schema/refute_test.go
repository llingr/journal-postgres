// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"testing"

	"github.com/llingr/journal-postgres/tests/harness"
)

// The tests in this file are not in the contract. Each pins a behaviour the
// schema permits and no rule forbids, found by attacking the versions and the owner's
// deletes.

// TestTheOwnerCanRewriteTheHead records that deleting the head of a stream frees its version
// for reuse, and nothing in the stream shows it was truncated and extended again.
func TestTheOwnerCanRewriteTheHead(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 3)

	commandTag, err := rig.OwnerPool().Exec(ctx,
		`DELETE FROM events WHERE stream_id = $1 AND version = 3`, streamID.String())
	harness.RequireNoError(t, err, "delete the head")
	if commandTag.RowsAffected() != 1 {
		t.Fatalf("delete removed %d rows, want 1", commandTag.RowsAffected())
	}

	replacement := harness.NewEvent(streamID, 3)
	replacement.EventType = "OrderCancelled"
	replacement.EventData = []byte(`{"reason":"rewritten"}`)
	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), replacement),
		"re-append version 3")

	stored, err := harness.ReadStream(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the rewritten stream")
	requireVersionsFromOne(t, stored, 3)
	if stored[2].EventType != "OrderCancelled" {
		t.Fatalf("version 3 is %q, want the replacement", stored[2].EventType)
	}
}

// TestTheOwnerCanResurrectAStreamAtVersionOne records that a stream whose
// rows were all deleted is indistinguishable from one that never existed: the
// next append is accepted at version 1.
func TestTheOwnerCanResurrectAStreamAtVersionOne(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 3)

	harness.RequireNoError(t, harness.DeleteStream(ctx, rig.OwnerPool(), streamID),
		"delete every event")

	revived := harness.NewEvent(streamID, 1)
	revived.EventType = "OrderPlacedAgain"
	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), revived),
		"append version 1 again")

	stored, err := harness.ReadStream(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the resurrected stream")
	requireVersionsFromOne(t, stored, 1)
}

// TestTheOwnerCanPunchAHoleMidStream records that nothing constrains a
// deletion to a prefix. Versions above the gap stay and appends continue from the surviving
// head; the store's reads refuse the stream from then on.
func TestTheOwnerCanPunchAHoleMidStream(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 4)

	_, err := rig.OwnerPool().Exec(ctx,
		`DELETE FROM events WHERE stream_id = $1 AND version = 2`, streamID.String())
	harness.RequireNoError(t, err, "delete a middle version")

	versions, err := harness.StreamVersions(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the versions after the hole")
	if len(versions) != 3 || versions[0] != 1 || versions[1] != 3 {
		t.Fatalf("stream holds %v, want [1 3 4]", versions)
	}

	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), harness.NewEvent(streamID, 5)),
		"append past the hole")
}

// TestOrphanedOutboxRowBlocksTheReappend records the interaction between the
// deliberate absence of a foreign key on the outbox and the resurrection of a
// deleted stream: a delete that removes events without the matching outbox
// rows leaves the stream unable to be re-appended.
func TestOrphanedOutboxRowBlocksTheReappend(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 1)

	harness.RequireNoError(t,
		harness.InsertOutboxRow(ctx, rig.AppPool(), streamID, 1, harness.DefaultAppendTs),
		"queue version 1")
	harness.RequireNoError(t, harness.DeleteStream(ctx, rig.OwnerPool(), streamID), "delete the event")

	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), harness.NewEvent(streamID, 1)),
		"re-append version 1")

	err := harness.InsertOutboxRow(ctx, rig.AppPool(), streamID, 1, harness.DefaultAppendTs)
	harness.RequireSQLState(t, err, harness.SQLStateUniqueViolation,
		harness.ConstraintOutboxPrimaryKey)
}

// TestOnConflictDoNothingSkipsARepeatSilently records that nothing but the primary key
// stops a repeated version, so a writer of its own that adds ON CONFLICT DO NOTHING loses the
// event without an error. The library never does.
func TestOnConflictDoNothingSkipsARepeatSilently(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 2)

	commandTag, err := rig.AppPool().Exec(ctx, `
		INSERT INTO events (stream_id, version, data_format, data_compression,
		                    data_encryption, event_append_ts, event_data, event_type,
		                    data_encryption_kid, metadata, partition_key)
		VALUES ($1, 2, 'json', 'none', 'none', $2, '\x41', 'OrderPlaced', '', '', '')
		ON CONFLICT (stream_id, version) DO NOTHING`, streamID.String(), harness.DefaultAppendTs)
	harness.RequireNoError(t, err, "insert a repeat with ON CONFLICT DO NOTHING")
	if commandTag.RowsAffected() != 0 {
		t.Fatalf("the repeat inserted %d rows, want 0", commandTag.RowsAffected())
	}
}
