// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llingr/journal-postgres/tests/harness"
)

// TestAppUpdatesContentNotKeys covers contract G1. The app may update the content of the
// state row, version 0, and never an event's, which the trigger refuses, nor any row's key,
// which the grant leaves out.
func TestAppUpdatesContentNotKeys(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 1)
	err := harness.InsertEvents(ctx, rig.AppPool(), harness.NewEvent(streamID, 0))
	harness.RequireNoError(t, err, "insert the state row")

	_, err = rig.AppPool().Exec(ctx,
		`UPDATE events SET metadata = '\x01' WHERE stream_id = $1 AND version = 0`,
		streamID.String())
	harness.RequireNoError(t, err, "update the state's content as the app")

	for name, role := range map[string]*pgxpool.Pool{"app": rig.AppPool(),
		"owner": rig.OwnerPool()} {
		t.Run("event_content_as_"+name, func(t *testing.T) {
			_, err := role.Exec(ctx,
				`UPDATE events SET metadata = '\x01' WHERE stream_id = $1 AND version = 1`,
				streamID.String())
			harness.RequireSQLState(t, err, harness.SQLStateInsufficientPrivilege, "")
		})
	}

	notGranted := []struct {
		name      string
		statement string
	}{
		{"stream_id", `UPDATE events SET stream_id = gen_random_uuid() WHERE stream_id = $1`},
		{"version", `UPDATE events SET version = 0 WHERE stream_id = $1`},
		{"partition_key", `UPDATE events SET partition_key = 'other' WHERE stream_id = $1`},
	}
	for _, attempt := range notGranted {
		t.Run(attempt.name, func(t *testing.T) {
			_, err := rig.AppPool().Exec(ctx, attempt.statement, streamID.String())
			harness.RequireSQLState(t, err, harness.SQLStateInsufficientPrivilege, "")
		})
	}

	versions, err := harness.StreamVersions(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the versions after the refused updates")
	if len(versions) != 2 || versions[0] != 0 || versions[1] != 1 {
		t.Fatalf("stream holds %v after the refused updates, want [0 1]", versions)
	}
}

// TestAppCannotDeleteEvents covers contract G2.
func TestAppCannotDeleteEvents(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 2)

	_, err := rig.AppPool().Exec(ctx, `DELETE FROM events WHERE stream_id = $1`, streamID.String())
	harness.RequireSQLState(t, err, harness.SQLStateInsufficientPrivilege, "")

	versions, err := harness.StreamVersions(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the versions after the refused delete")
	if len(versions) != 2 {
		t.Fatalf("stream holds %v after the refused delete, want two versions", versions)
	}
}

// TestAppCanInsertAndSelectEvents covers contract G3.
func TestAppCanInsertAndSelectEvents(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := uuid.New()

	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), harness.NewEvent(streamID, 1)),
		"append as app")

	stored, err := harness.ReadStream(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "select as app")
	if len(stored) != 1 {
		t.Fatalf("app read %d events, want 1", len(stored))
	}
}

// TestAppHasNoDataDefinitionRights covers contract G10.
func TestAppHasNoDataDefinitionRights(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	attempts := []struct {
		name      string
		statement string
	}{
		{"alter_events", `ALTER TABLE events ADD COLUMN injected INTEGER`},
		{"alter_events_drop_constraint", `ALTER TABLE events DROP CONSTRAINT chk_events_version`},
		{"drop_events_outbox", `DROP TABLE events_outbox`},
		{"truncate_events", `TRUNCATE events`},
		// Qualified with the schema under test: the app holds USAGE on it and
		// nothing more, so the refusal is the migration's grant, not the
		// cluster-wide default on public.
		{"create_table", `CREATE TABLE ` + rig.SchemaName + `.injected_by_app (identifier INTEGER)`},
		{"create_view_over_events", `CREATE VIEW ` + rig.SchemaName +
			`.injected_view AS SELECT event_data FROM events`},
	}

	for _, attempt := range attempts {
		t.Run(attempt.name, func(t *testing.T) {
			_, err := rig.AppPool().Exec(ctx, attempt.statement)
			harness.RequireSQLState(t, err, harness.SQLStateInsufficientPrivilege, "")
		})
	}

	// A GRANT by a role that holds no grant option is a warning and a no-op, not
	// an error, so the fence is the privilege that fails to arrive.
	t.Run("granting_itself_delete_confers_nothing", func(t *testing.T) {
		streamID := appendStream(t, rig, 1)

		_, err := rig.AppPool().Exec(ctx, `GRANT DELETE ON events TO `+harness.AppUsername)
		harness.RequireNoError(t, err, "attempt a self-grant")

		_, err = rig.AppPool().Exec(ctx, `DELETE FROM events WHERE stream_id = $1`, streamID.String())
		harness.RequireSQLState(t, err, harness.SQLStateInsufficientPrivilege, "")
	})
}

// TestRolesAreDistinctSessions proves each pool authenticates as its own role,
// so a privilege assertion cannot pass because it ran as the owner.
func TestRolesAreDistinctSessions(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	pools := map[string]*pgxpool.Pool{
		harness.OwnerUsername: rig.OwnerPool(),
		harness.AppUsername:   rig.AppPool(),
	}

	for wantUsername, pool := range pools {
		t.Run(wantUsername, func(t *testing.T) {
			var (
				sessionUser string
				currentUser string
				searchPath  string
			)
			err := pool.QueryRow(ctx,
				`SELECT session_user, current_user, current_setting('search_path')`).
				Scan(&sessionUser, &currentUser, &searchPath)
			harness.RequireNoError(t, err, "read the session identity")

			if sessionUser != wantUsername || currentUser != wantUsername {
				t.Fatalf("pool authenticates as session_user %q and current_user %q, want %q",
					sessionUser, currentUser, wantUsername)
			}
			if searchPath != rig.SchemaName {
				t.Fatalf("search_path is %q, want %q", searchPath, rig.SchemaName)
			}
		})
	}
}
