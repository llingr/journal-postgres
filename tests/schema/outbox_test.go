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

// TestAppOwnsTheOutbox covers contract H1.
func TestAppOwnsTheOutbox(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 1)

	harness.RequireNoError(t,
		harness.InsertOutboxRow(ctx, rig.AppPool(), streamID, 1, harness.DefaultAppendTs),
		"insert an outbox row as app")

	var pending int64
	err := rig.AppPool().QueryRow(ctx,
		`SELECT count(*) FROM events_outbox WHERE stream_id = $1`, streamID.String()).Scan(&pending)
	harness.RequireNoError(t, err, "select the outbox as app")
	if pending != 1 {
		t.Fatalf("outbox holds %d rows, want 1", pending)
	}

	commandTag, err := rig.AppPool().Exec(ctx,
		`DELETE FROM events_outbox WHERE stream_id = $1`, streamID.String())
	harness.RequireNoError(t, err, "delete from the outbox as app")
	if commandTag.RowsAffected() != 1 {
		t.Fatalf("delete removed %d rows, want 1", commandTag.RowsAffected())
	}
}

// TestOutboxRefusesDuplicates covers contract H4.
func TestOutboxRefusesDuplicates(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 1)

	harness.RequireNoError(t,
		harness.InsertOutboxRow(ctx, rig.AppPool(), streamID, 1, harness.DefaultAppendTs),
		"insert the first outbox row")

	err := harness.InsertOutboxRow(ctx, rig.AppPool(), streamID, 1,
		harness.DefaultAppendTs.Add(time.Hour))
	harness.RequireSQLState(t, err, harness.SQLStateUniqueViolation,
		harness.ConstraintOutboxPrimaryKey)
}

// TestOutboxHasNoForeignKey covers contract H5. The absence of a foreign key is
// deliberate, so an orphan must survive its event.
func TestOutboxHasNoForeignKey(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 2)

	harness.RequireNoError(t,
		harness.InsertOutboxRow(ctx, rig.AppPool(), streamID, 1, harness.DefaultAppendTs),
		"insert an outbox row")

	harness.RequireNoError(t, harness.DeleteStream(ctx, rig.OwnerPool(), streamID),
		"delete every event")

	var pending int64
	err := rig.AppPool().QueryRow(ctx,
		`SELECT count(*) FROM events_outbox WHERE stream_id = $1`, streamID.String()).Scan(&pending)
	harness.RequireNoError(t, err, "count the surviving outbox rows")
	if pending != 1 {
		t.Fatalf("outbox holds %d rows after the events were deleted, want the orphan to "+
			"survive", pending)
	}

	t.Run("outbox_accepts_a_row_for_an_event_that_never_existed", func(t *testing.T) {
		err = harness.InsertOutboxRow(ctx, rig.AppPool(), uuid.New(), 42, harness.DefaultAppendTs)
		harness.RequireNoError(t, err, "insert an outbox row naming no event")
	})

	var foreignKeys int64
	err = rig.OwnerPool().QueryRow(ctx, `
		SELECT count(*)
		FROM   pg_constraint
		JOIN   pg_class ON pg_class.oid = pg_constraint.conrelid
		JOIN   pg_namespace ON pg_namespace.oid = pg_class.relnamespace
		WHERE  pg_namespace.nspname = $1 AND pg_class.relname = 'events_outbox'
		       AND pg_constraint.contype = 'f'`, rig.SchemaName).Scan(&foreignKeys)
	harness.RequireNoError(t, err, "count the outbox foreign keys")
	if foreignKeys != 0 {
		t.Fatalf("events_outbox carries %d foreign keys; the absence is deliberate", foreignKeys)
	}
}
