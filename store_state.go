// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/llingr/journal/persistence"
)

// errStateMetadata is a state row whose metadata is not the two versions the store writes
var errStateMetadata = errors.New("the state row's metadata is not its versions")

// errStateVersions is a state folded to no event, or with a negative state version
var errStateVersions = errors.New("the state's versions cannot be stored")

// stateMetadataBytes is the state row's metadata: the version folded to, then the state
// version, each four bytes big-endian
const stateMetadataBytes = 8

// SaveState writes the state row unless the stored one is folded to a later version
func (st *store) SaveState(ctx context.Context, state persistence.PersistedState) (bool, error) {
	err := checkState(state)
	if err != nil {
		return false, err
	}
	commandTag, err := st.pool.Exec(ctx, st.queries.saveState, stateArgs(state))
	if err != nil {
		return false, fmt.Errorf("save the state of stream %s at version %d: %w",
			state.StreamID, state.Version, st.storeError(err))
	}
	return commandTag.RowsAffected() == 1, nil
}

// checkState refuses a state folded to no event, or with a negative state version, which the
// metadata could not order
func checkState(state persistence.PersistedState) error {
	if state.Version < 1 || state.StateVersion < 0 {
		return fmt.Errorf("save the state of stream %s: %w: version %d, state version %d",
			state.StreamID, errStateVersions, state.Version, state.StateVersion)
	}
	return nil
}

// stateArgs are the state row's columns as the save-state statement names them
func stateArgs(state persistence.PersistedState) pgx.NamedArgs {
	return pgx.NamedArgs{
		"stream_id":           uuidValue(state.StreamID),
		"data_format":         string(state.DataFormat),
		"data_compression":    string(state.DataCompression),
		"data_encryption":     string(state.DataEncryption),
		"event_append_ts":     state.LastAppendTs,
		"event_data":          notNullBytes(state.StateData),
		"event_type":          state.LastEventType,
		"data_encryption_kid": state.DataEncryptionKid,
		"metadata":            stateMetadata(state.Version, state.StateVersion),
		"partition_key":       state.PartitionKey,
	}
}

// stateMetadata is the two versions as the state row stores them. Big-endian, so the first
// four bytes compare as the versions do, which save-state relies on; neither is negative.
func stateMetadata(version int32, stateVersion int32) []byte {
	metadata := make([]byte, stateMetadataBytes)
	binary.BigEndian.PutUint32(metadata[:4], uint32(version))
	binary.BigEndian.PutUint32(metadata[4:], uint32(stateVersion))
	return metadata
}

// stateVersions reads the two versions back from the state row's metadata
func stateVersions(metadata []byte) (int32, int32, error) {
	if len(metadata) != stateMetadataBytes {
		return 0, 0, fmt.Errorf("%w: %d bytes", errStateMetadata, len(metadata))
	}
	return int32(binary.BigEndian.Uint32(metadata[:4])),
		int32(binary.BigEndian.Uint32(metadata[4:])), nil
}
