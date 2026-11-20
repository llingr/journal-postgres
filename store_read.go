// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"math"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// QueryStream returns up to limit of one stream's events in ascending version order. A stream
// that does not exist reads as no events rather than as an error, because an unwritten stream
// and an empty one are the same thing to a caller folding state.
func (st *store) QueryStream(ctx context.Context, streamID uuid.UUID, fromVersion int32,
	limit int) ([]persistence.PersistedEvent, error) {
	if fromVersion < 1 {
		return nil, fmt.Errorf("read stream %s from version %d: %w", streamID, fromVersion,
			streams.ErrVersionsNotConsecutive)
	}
	return st.readContiguous(ctx, streamID, fromVersion, math.MaxInt32, limit)
}

// QueryState returns the stream's state row and its last version, read in one statement
func (st *store) QueryState(ctx context.Context,
	streamID uuid.UUID) (persistence.PersistedState, int32, error) {
	var (
		head              int32
		eventType         pgtype.Text
		stateData         []byte
		dataFormat        pgtype.Text
		dataCompression   pgtype.Text
		dataEncryption    pgtype.Text
		dataEncryptionKid pgtype.Text
		metadata          []byte
		appendTs          pgtype.Timestamptz
		partitionKey      pgtype.Text
	)
	err := st.pool.QueryRow(ctx, st.queries.readState, pgx.NamedArgs{
		"stream_id": uuidValue(streamID),
	}).Scan(&head, &eventType, &stateData, &dataFormat, &dataCompression, &dataEncryption,
		&dataEncryptionKid, &metadata, &appendTs, &partitionKey)
	if err != nil {
		return persistence.PersistedState{}, 0, fmt.Errorf("read the state of stream %s: %w",
			streamID, st.storeError(err))
	}
	if !eventType.Valid {
		return persistence.PersistedState{}, head, nil
	}

	version, stateVersion, err := stateVersions(metadata)
	if err != nil {
		return persistence.PersistedState{}, 0, fmt.Errorf("read the state of stream %s: %w",
			streamID, err)
	}
	return persistence.PersistedState{
		StreamID:          streamID,
		Version:           version,
		StateVersion:      stateVersion,
		LastEventType:     eventType.String,
		LastAppendTs:      appendTs.Time,
		StateData:         stateData,
		DataFormat:        codec.DataFormat(dataFormat.String),
		DataCompression:   compress.DataCompression(dataCompression.String),
		DataEncryption:    encrypt.DataEncryption(dataEncryption.String),
		DataEncryptionKid: dataEncryptionKid.String,
		PartitionKey:      partitionKey.String,
	}, head, nil
}

// QueryHead returns the stream's last version, 0 for a stream with no events
func (st *store) QueryHead(ctx context.Context, streamID uuid.UUID) (int32, error) {
	var head int32
	err := st.pool.QueryRow(ctx, st.queries.streamHead, pgx.NamedArgs{
		"stream_id": uuidValue(streamID),
	}).Scan(&head)
	if err != nil {
		return 0, fmt.Errorf("read the head of stream %s: %w", streamID, st.storeError(err))
	}
	return head, nil
}

// QueryStreamRange returns one stream's events from fromVersion through throughVersion
func (st *store) QueryStreamRange(ctx context.Context, streamID uuid.UUID, fromVersion int32,
	throughVersion int32) ([]persistence.PersistedEvent, error) {
	if fromVersion < 1 {
		return nil, fmt.Errorf("read stream %s from version %d: %w", streamID, fromVersion,
			streams.ErrVersionsNotConsecutive)
	}
	return st.readContiguous(ctx, streamID, fromVersion, throughVersion,
		max(int(throughVersion-fromVersion)+1, 0))
}

// readContiguous reads up to limit events and refuses them unless they start at fromVersion
// and have no gaps, so a caller never folds part of a stream
func (st *store) readContiguous(ctx context.Context, streamID uuid.UUID, fromVersion int32,
	throughVersion int32, limit int) ([]persistence.PersistedEvent, error) {
	rows, err := st.pool.Query(ctx, st.queries.readStream, pgx.NamedArgs{
		"stream_id":       uuidValue(streamID),
		"from_version":    fromVersion,
		"through_version": throughVersion,
		"row_limit":       limit,
	})
	if err != nil {
		return nil, fmt.Errorf("read stream %s: %w", streamID, st.storeError(err))
	}
	defer rows.Close()

	var read []persistence.PersistedEvent
	for rows.Next() {
		event, scanErr := scanEvent(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("read stream %s: %w", streamID, scanErr)
		}
		expected := fromVersion + int32(len(read))
		if event.Version != expected {
			return nil, fmt.Errorf("read stream %s: %w: version %d",
				streamID, streams.ErrVersionMissing, expected)
		}
		read = append(read, event)
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("read stream %s: %w", streamID, st.storeError(err))
	}
	return read, nil
}

// scanEvent maps one row. Row mapping is a store internal and never escapes.
func scanEvent(rows pgx.Rows) (persistence.PersistedEvent, error) {
	var (
		event       persistence.PersistedEvent
		streamValue pgtype.UUID
	)
	err := rows.Scan(&streamValue, &event.Version, &event.EventType, &event.EventData,
		&event.DataFormat, &event.DataCompression, &event.DataEncryption, &event.DataEncryptionKid,
		&event.Metadata, &event.EventAppendTs, &event.PartitionKey)
	if err != nil {
		return persistence.PersistedEvent{}, fmt.Errorf("scan event: %w", err)
	}

	event.StreamID = streamValue.Bytes
	return event, nil
}
