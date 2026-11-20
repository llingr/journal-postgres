// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"testing"

	journalschema "github.com/llingr/journal-postgres/schema"
	"github.com/llingr/journal-postgres/tests/harness"
)

// finalMigrationVersion is 0003_events_outbox.
const finalMigrationVersion = 3

// TestMigrationsApplyInOrder covers contract A1.
func TestMigrationsApplyInOrder(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	var (
		version int64
		dirty   bool
	)
	err := rig.OwnerPool().QueryRow(ctx,
		`SELECT version, dirty FROM `+journalschema.MigrationsTable).Scan(&version, &dirty)
	harness.RequireNoError(t, err, "read "+journalschema.MigrationsTable)

	if version != finalMigrationVersion {
		t.Fatalf("schema is at migration %d, want %d", version, finalMigrationVersion)
	}
	if dirty {
		t.Fatalf("%s is dirty after a clean run", journalschema.MigrationsTable)
	}
}

// TestMigratingTwiceIsNoOp covers contract A2.
func TestMigratingTwiceIsNoOp(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 2)

	harness.RequireNoError(t, rig.Migrate(), "second migration run")

	versions, err := harness.StreamVersions(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the stream after the second run")
	if len(versions) != 2 {
		t.Fatalf("stream holds %v after the second run, want both versions", versions)
	}
}

// TestEveryReferencedObjectExists covers contract A4.
func TestEveryReferencedObjectExists(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	tableNames := []string{
		"events", "events_outbox",
	}
	for _, tableName := range tableNames {
		t.Run("table_"+tableName, func(t *testing.T) {
			var exists bool
			err := rig.OwnerPool().QueryRow(ctx, `
				SELECT EXISTS (SELECT 1 FROM pg_class
				               JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
				               WHERE pg_namespace.nspname = $1 AND pg_class.relname = $2
				                 AND pg_class.relkind = 'r')`,
				rig.SchemaName, tableName).Scan(&exists)
			harness.RequireNoError(t, err, "look up "+tableName)
			if !exists {
				t.Fatalf("table %s is absent", tableName)
			}
		})
	}
}

// TestPayloadColumnsAreNotCompressedByPostgres covers contract A5: the library compresses
// event_data and metadata itself, so Postgres stores them as given.
func TestPayloadColumnsAreNotCompressedByPostgres(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	for _, column := range []string{"event_data", "metadata"} {
		var storage string
		err := rig.OwnerPool().QueryRow(ctx, `
			SELECT attstorage::TEXT FROM pg_attribute
			WHERE  attrelid = 'events'::REGCLASS AND attname = $1`, column).Scan(&storage)
		harness.RequireNoError(t, err, "read the storage of "+column)
		if storage != "e" {
			t.Errorf("%s has storage %q, want external, e", column, storage)
		}
	}
}
