// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// QueryPendingStreams lists streams with a row appended from appendedAfter to before
// appendedBefore, reading at most limit rows, so a stream with several such rows is listed
// once and the list may be shorter
func (st *store) QueryPendingStreams(ctx context.Context, appendedAfter time.Time,
	appendedBefore time.Time, limit int) ([]uuid.UUID, error) {
	rows, err := st.pool.Query(ctx, st.queries.outboxStreams, pgx.NamedArgs{
		"appended_after":  appendedAfter,
		"appended_before": appendedBefore,
		"row_limit":       limit,
	})
	if err != nil {
		return nil, fmt.Errorf("list pending streams: %w", st.storeError(err))
	}
	defer rows.Close()

	var streams []uuid.UUID
	listed := make(map[uuid.UUID]bool)
	for rows.Next() {
		var streamValue pgtype.UUID
		err = rows.Scan(&streamValue)
		if err != nil {
			return nil, fmt.Errorf("list pending streams: %w", err)
		}
		streamID := uuid.UUID(streamValue.Bytes)
		if !listed[streamID] {
			listed[streamID] = true
			streams = append(streams, streamID)
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("list pending streams: %w", st.storeError(err))
	}
	return streams, nil
}

// QueryPending returns one stream's unpublished versions in order
func (st *store) QueryPending(ctx context.Context, streamID uuid.UUID) ([]int32, error) {
	rows, err := st.pool.Query(ctx, st.queries.outboxVersions, pgx.NamedArgs{
		"stream_id": uuidValue(streamID),
	})
	if err != nil {
		return nil, fmt.Errorf("read pending rows of stream %s: %w", streamID, st.storeError(err))
	}
	var pending []int32
	pending, err = pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		return nil, fmt.Errorf("read pending rows of stream %s: %w", streamID, st.storeError(err))
	}
	return pending, nil
}

// DeletePublished removes the stream's pending rows up to and including throughVersion
func (st *store) DeletePublished(ctx context.Context, streamID uuid.UUID,
	throughVersion int32) error {
	_, err := st.pool.Exec(ctx, st.queries.deletePublished, pgx.NamedArgs{
		"stream_id": uuidValue(streamID),
		"version":   throughVersion,
	})
	if err != nil {
		return fmt.Errorf("clear stream %s through version %d: %w",
			streamID, throughVersion, st.storeError(err))
	}
	return nil
}
