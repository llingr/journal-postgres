// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"
)

// Append sends the events, their outbox rows, the read of the stream's earliest unpublished
// version and the stream's state as one batch: one round trip, which Postgres runs as one
// implicit transaction, so any failure leaves none of it, and the state is never behind its
// events. The versions must run on from 1 or above; the primary key refuses one another
// writer took first.
func (st *store) Append(ctx context.Context, request persistence.AppendRequest) (int32, error) {
	if len(request.Events) == 0 {
		return 0, nil
	}
	firstVersion := request.Events[0].Version
	for index, event := range request.Events {
		if firstVersion < 1 || event.Version != firstVersion+int32(index) {
			return 0, fmt.Errorf("append to stream %s: %w: version %d at position %d",
				request.StreamID, streams.ErrVersionsNotConsecutive, event.Version, index)
		}
	}
	lastVersion := request.Events[len(request.Events)-1].Version
	if request.State != nil {
		err := checkState(*request.State)
		if err != nil {
			return 0, err
		}
		if request.State.Version != lastVersion || request.State.StreamID != request.StreamID {
			return 0, fmt.Errorf("append to stream %s: %w: the state is folded to stream %s "+
				"version %d, not version %d", request.StreamID, errStateVersions,
				request.State.StreamID, request.State.Version, lastVersion)
		}
	}

	batch := &pgx.Batch{}
	columns := eventColumns(request.Events)
	batch.Queue(st.queries.insertEvents, pgx.NamedArgs{
		"stream_id":            uuidValue(request.StreamID),
		"versions":             columns.versions,
		"data_formats":         columns.dataFormats,
		"data_compressions":    columns.dataCompressions,
		"data_encryptions":     columns.dataEncryptions,
		"event_append_ts":      request.AppendTs,
		"event_data":           columns.eventData,
		"event_types":          columns.eventTypes,
		"data_encryption_kids": columns.dataEncryptionKids,
		"metadata":             columns.metadata,
		"partition_keys":       columns.partitionKeys,
	})
	if request.Publish {
		batch.Queue(st.queries.insertOutbox, pgx.NamedArgs{
			"stream_id":       uuidValue(request.StreamID),
			"versions":        columns.versions,
			"event_append_ts": request.AppendTs,
		})
		batch.Queue(st.queries.earliestOutbox, pgx.NamedArgs{
			"stream_id": uuidValue(request.StreamID),
			"version":   firstVersion,
		})
	}

	if request.State != nil {
		batch.Queue(st.queries.saveState, stateArgs(*request.State))
	}

	results := st.pool.SendBatch(ctx, batch)
	earliestPending, err := readAppendResults(results, request.Publish, request.State != nil)
	err = errors.Join(err, results.Close())
	if err != nil {
		return 0, fmt.Errorf("append to stream %s from version %d: %w",
			request.StreamID, firstVersion, st.storeError(err))
	}
	return earliestPending, nil
}

// readAppendResults takes the events insert's result, then for a published append the outbox
// insert's and the earliest unpublished version, 0 for none, then the state's
func readAppendResults(results pgx.BatchResults, published bool, withState bool) (int32,
	error) {
	_, err := results.Exec()
	if err != nil {
		return 0, err
	}

	var earliestPending int32
	if published {
		_, err = results.Exec()
		if err != nil {
			return 0, err
		}
		err = results.QueryRow().Scan(&earliestPending)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return 0, err
		}
	}

	// the events' versions were free, so no stored state is folded past them and this one is
	// always written
	if withState {
		_, err = results.Exec()
		if err != nil {
			return 0, err
		}
	}
	return earliestPending, nil
}

// appendedColumns holds the events column by column, one array per column for the insert
type appendedColumns struct {
	versions           []int32
	dataFormats        []string
	dataCompressions   []string
	dataEncryptions    []string
	eventData          [][]byte
	eventTypes         []string
	dataEncryptionKids []string
	metadata           [][]byte
	partitionKeys      []string
}

func eventColumns(appended []persistence.PersistedEvent) appendedColumns {
	columns := appendedColumns{
		versions:           make([]int32, len(appended)),
		dataFormats:        make([]string, len(appended)),
		dataCompressions:   make([]string, len(appended)),
		dataEncryptions:    make([]string, len(appended)),
		eventData:          make([][]byte, len(appended)),
		eventTypes:         make([]string, len(appended)),
		dataEncryptionKids: make([]string, len(appended)),
		metadata:           make([][]byte, len(appended)),
		partitionKeys:      make([]string, len(appended)),
	}
	for index, event := range appended {
		columns.versions[index] = event.Version
		columns.dataFormats[index] = string(event.DataFormat)
		columns.dataCompressions[index] = string(event.DataCompression)
		columns.dataEncryptions[index] = string(event.DataEncryption)
		columns.eventData[index] = notNullBytes(event.EventData)
		columns.eventTypes[index] = event.EventType
		columns.dataEncryptionKids[index] = event.DataEncryptionKid
		columns.metadata[index] = notNullBytes(event.Metadata)
		columns.partitionKeys[index] = event.PartitionKey
	}
	return columns
}

// notNullBytes sends no bytes as empty bytes for a NOT NULL column, as pgx sends a nil slice
// as NULL
func notNullBytes(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}
