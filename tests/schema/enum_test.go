// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal-postgres/tests/harness"
)

// TestEventsRejectUnlistedCodecNames covers contract D1 and D2.
func TestEventsRejectUnlistedCodecNames(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	attempts := []struct {
		name string
		vary func(event *harness.Event)
	}{
		{"data_format_empty", func(event *harness.Event) {
			event.DataFormat = ""
		}},
		{"data_format_unlisted", func(event *harness.Event) {
			event.DataFormat = "toml"
		}},
		{"data_format_capitals", func(event *harness.Event) {
			event.DataFormat = "JSON"
		}},
		{"data_compression_empty", func(event *harness.Event) {
			event.DataCompression = ""
		}},
		{"data_compression_unlisted", func(event *harness.Event) {
			event.DataCompression = "brotli"
		}},
		{"data_encryption_empty", func(event *harness.Event) {
			event.DataEncryption = ""
			event.DataEncryptionKid = "key-1"
		}},
		{"data_encryption_unlisted", func(event *harness.Event) {
			event.DataEncryption = "rot13"
			event.DataEncryptionKid = "key-1"
		}},
	}

	for _, attempt := range attempts {
		t.Run(attempt.name, func(t *testing.T) {
			event := harness.NewEvent(uuid.New(), 1)
			attempt.vary(&event)
			err := harness.InsertEvents(ctx, rig.AppPool(), event)
			harness.RequireSQLState(t, err, harness.SQLStateInvalidTextRepresentation, "")
		})
	}
}

// TestCodecColumnsAreEnums covers contract D1: each codec column has its own enum type.
func TestCodecColumnsAreEnums(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	for column, wantType := range map[string]string{
		"data_format":      "events_data_format",
		"data_compression": "events_data_compression",
		"data_encryption":  "events_data_encryption",
	} {
		t.Run(column, func(t *testing.T) {
			var typeName, typeKind string
			err := rig.OwnerPool().QueryRow(ctx, `
				SELECT pg_type.typname, pg_type.typtype::TEXT
				FROM   pg_attribute
				JOIN   pg_type ON pg_type.oid = pg_attribute.atttypid
				WHERE  pg_attribute.attrelid = 'events'::REGCLASS AND pg_attribute.attname = $1`,
				column).Scan(&typeName, &typeKind)
			harness.RequireNoError(t, err, "read the type of "+column)
			if typeName != wantType || typeKind != "e" {
				t.Fatalf("%s is %s of kind %q, want the enum %s", column, typeName, typeKind,
					wantType)
			}
		})
	}
}

// TestEventsEncryptionPairing covers contract D3, D4 and D5.
func TestEventsEncryptionPairing(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	t.Run("none_with_a_kid_refused", func(t *testing.T) {
		event := harness.NewEvent(uuid.New(), 1)
		event.DataEncryption = harness.EncryptionNone
		event.DataEncryptionKid = "key-1"
		err := harness.InsertEvents(ctx, rig.AppPool(), event)
		harness.RequireSQLState(t, err, harness.SQLStateCheckViolation,
			harness.ConstraintEventsEncryptionPairing)
	})

	t.Run("encrypted_with_an_empty_kid_refused", func(t *testing.T) {
		event := harness.NewEvent(uuid.New(), 1)
		event.DataEncryption = harness.EncryptionAesGcm
		event.DataEncryptionKid = ""
		err := harness.InsertEvents(ctx, rig.AppPool(), event)
		harness.RequireSQLState(t, err, harness.SQLStateCheckViolation,
			harness.ConstraintEventsEncryptionPairing)
	})

	t.Run("none_with_an_empty_kid_accepted", func(t *testing.T) {
		event := harness.NewEvent(uuid.New(), 1)
		event.DataEncryption = harness.EncryptionNone
		event.DataEncryptionKid = ""
		harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), event),
			"append unencrypted")
	})

	t.Run("encrypted_with_a_kid_accepted", func(t *testing.T) {
		event := harness.NewEvent(uuid.New(), 1)
		event.DataEncryption = harness.EncryptionAesGcm
		event.DataEncryptionKid = "key-1"
		harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), event), "append encrypted")
	})

	t.Run("no_kid_is_the_empty_string", func(t *testing.T) {
		_, err := rig.AppPool().Exec(ctx, `
			INSERT INTO events (stream_id, version, data_format, data_compression,
			                    data_encryption, event_append_ts, event_data, event_type,
			                    data_encryption_kid, metadata, partition_key)
			VALUES ($1, 1, 'json', 'none', 'none', $2, '\x41', 'OrderPlaced', NULL, '', '')`,
			uuid.New().String(), harness.DefaultAppendTs)
		harness.RequireSQLState(t, err, harness.SQLStateNotNullViolation, "")
	})

}

// TestMetadataAndPartitionKeyAreNeverNull covers contract C1: no metadata is empty bytes and
// the stream id's key is the empty string, and NULL in either is refused with the error naming
// the column.
func TestMetadataAndPartitionKeyAreNeverNull(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	for column, values := range map[string]string{
		"metadata":      `'', NULL, ''`,
		"partition_key": `'', '', NULL`,
	} {
		t.Run(column, func(t *testing.T) {
			_, err := rig.AppPool().Exec(ctx, `
				INSERT INTO events (stream_id, version, data_format, data_compression,
				                    data_encryption, event_append_ts, event_data, event_type,
				                    data_encryption_kid, metadata, partition_key)
				VALUES ($1, 1, 'json', 'none', 'none', $2, '\x41', 'OrderPlaced', `+values+`)`,
				uuid.New().String(), harness.DefaultAppendTs)
			harness.RequireSQLState(t, err, harness.SQLStateNotNullViolation, "")
			postgresError, _ := harness.AsPostgresError(err)
			if postgresError.ColumnName != column {
				t.Fatalf("a NULL %s is reported on column %q: %s", column,
					postgresError.ColumnName, postgresError.Message)
			}
		})
	}
}
