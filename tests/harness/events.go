// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// Executor is satisfied by a pool and by a transaction, so every helper works
// inside or outside an explicit transaction.
type Executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
}

// DefaultAppendTs is the fixed append timestamp used wherever the value is not
// the thing under test.
var DefaultAppendTs = time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC)

// Data format, compression and encryption names the events CHECK lists admit.
const (
	FormatJSON           = "json"
	FormatText           = "text"
	CompressionNone      = "none"
	CompressionZstandard = "zstd"
	EncryptionNone       = "none"
	EncryptionAesGcm     = "aes-gcm"
)

// Event is one row destined for the events table.
type Event struct {
	StreamID          uuid.UUID
	Version           int32
	EventType         string
	EventData         []byte
	DataFormat        string
	DataCompression   string
	DataEncryption    string
	DataEncryptionKid string
	Metadata          []byte
	EventAppendTs     time.Time
	PartitionKey      string
}

// NewEvent returns an event carrying the defaults every field-specific test
// varies from.
func NewEvent(streamID uuid.UUID, version int32) Event {
	return Event{
		StreamID:        streamID,
		Version:         version,
		EventType:       "OrderPlaced",
		EventData:       []byte(`{"orderId":1}`),
		DataFormat:      FormatJSON,
		DataCompression: CompressionNone,
		DataEncryption:  EncryptionNone,
		EventAppendTs:   DefaultAppendTs,
	}
}

// StoredEvent is one row read back from the events table.
type StoredEvent struct {
	StreamID          uuid.UUID
	Version           int32
	EventType         string
	EventData         []byte
	DataFormat        string
	DataCompression   string
	DataEncryption    string
	DataEncryptionKid string
	Metadata          []byte
	EventAppendTs     time.Time
	PartitionKey      string
}

var eventColumnNames = []string{
	"stream_id", "version",
	"data_format", "data_compression", "data_encryption", "event_append_ts",
	"event_data", "event_type", "data_encryption_kid", "metadata", "partition_key",
}

const columnsPerEvent = 11

// InsertEvents appends every supplied event in one INSERT statement.
func InsertEvents(ctx context.Context, executor Executor, events ...Event) error {
	if len(events) == 0 {
		return fmt.Errorf("no events supplied")
	}

	var rowPlaceholders []string
	arguments := make([]any, 0, len(events)*columnsPerEvent)
	for index, event := range events {
		base := index * columnsPerEvent
		placeholders := make([]string, columnsPerEvent)
		for column := range columnsPerEvent {
			placeholders[column] = fmt.Sprintf("$%d", base+column+1)
		}
		rowPlaceholders = append(rowPlaceholders, "("+strings.Join(placeholders, ", ")+")")
		arguments = append(arguments,
			uuidValue(event.StreamID), event.Version,
			event.DataFormat, event.DataCompression, event.DataEncryption, event.EventAppendTs,
			event.EventData, event.EventType, event.DataEncryptionKid, metadataValue(event.Metadata),
			event.PartitionKey)
	}

	statement := fmt.Sprintf("INSERT INTO events (%s) VALUES %s",
		strings.Join(eventColumnNames, ", "), strings.Join(rowPlaceholders, ", "))
	_, err := executor.Exec(ctx, statement, arguments...)
	return err
}

// ReadStream returns every stored row for a stream in version order.
func ReadStream(ctx context.Context, executor Executor, streamID uuid.UUID) ([]StoredEvent, error) {
	rows, err := executor.Query(ctx, `
		SELECT stream_id, version, event_type, event_data,
		       data_format, data_compression, data_encryption, data_encryption_kid,
		       metadata, event_append_ts, partition_key
		FROM   events
		WHERE  stream_id = $1
		ORDER BY version`, uuidValue(streamID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stored []StoredEvent
	for rows.Next() {
		var (
			event       StoredEvent
			streamValue pgtype.UUID
		)
		err = rows.Scan(&streamValue, &event.Version, &event.EventType, &event.EventData,
			&event.DataFormat, &event.DataCompression, &event.DataEncryption, &event.DataEncryptionKid,
			&event.Metadata, &event.EventAppendTs, &event.PartitionKey)
		if err != nil {
			return nil, err
		}
		event.StreamID = streamValue.Bytes
		stored = append(stored, event)
	}
	return stored, rows.Err()
}

// StreamVersions returns the versions present for a stream, in order.
func StreamVersions(ctx context.Context, executor Executor, streamID uuid.UUID) ([]int32, error) {
	rows, err := executor.Query(ctx,
		`SELECT version FROM events WHERE stream_id = $1 ORDER BY version`, uuidValue(streamID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var versions []int32
	for rows.Next() {
		var version int32
		if err = rows.Scan(&version); err != nil {
			return nil, err
		}
		versions = append(versions, version)
	}
	return versions, rows.Err()
}

// DeleteStream removes every event of a stream. It needs the owner role.
func DeleteStream(ctx context.Context, executor Executor, streamID uuid.UUID) error {
	_, err := executor.Exec(ctx, `DELETE FROM events WHERE stream_id = $1`, uuidValue(streamID))
	return err
}

// InsertOutboxRow writes one events_outbox row.
func InsertOutboxRow(ctx context.Context, executor Executor, streamID uuid.UUID, version int32,
	appendTs time.Time) error {
	_, err := executor.Exec(ctx,
		`INSERT INTO events_outbox (stream_id, version, event_append_ts) VALUES ($1, $2, $3)`,
		uuidValue(streamID), version, appendTs)
	return err
}

func uuidValue(value uuid.UUID) pgtype.UUID {
	return pgtype.UUID{
		Bytes: value,
		Valid: true,
	}
}

// metadataValue sends no metadata as empty bytes, as pgx sends a nil slice as NULL
func metadataValue(metadata []byte) []byte {
	if metadata == nil {
		return []byte{}
	}
	return metadata
}
