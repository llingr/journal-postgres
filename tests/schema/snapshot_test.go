// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/formatjson"
	"github.com/llingr/journal/streams"

	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/tests/harness"
)

// notebook is a state long enough to be compressed
type notebook struct {
	Notes []string `json:"notes"`
}

// TestTheStateRowIsVersion0WithItsVersionsInTheMetadata: the stored state is the row of
// version 0, compressed as an event is, its metadata the version folded to and the state
// version, each four bytes big-endian, which save-state compares
func TestTheStateRowIsVersion0WithItsVersionsInTheMetadata(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	eventStore := journal.NewStore(postgres.NewBackend(rig.AppPool()),
		journal.WithDataCompression(compress.Zstd))
	notebooks := journal.NewRepository(eventStore,
		journal.WithTransitions(journal.Transition(
			func(state notebook, note *noted) (notebook, error) {
				state.Notes = append(state.Notes, note.Text)
				return state, nil
			})),
		journal.WithSnapshot(streams.StateCodec[notebook]{
			Version: 3,
			Format:  formatjson.Format,
			Marshal: func(state notebook) ([]byte, error) {
				return formatjson.Marshal(state)
			},
			Unmarshal: func(encoded []byte) (notebook, error) {
				var state notebook
				err := formatjson.Unmarshal(encoded, &state)
				return state, err
			},
		}))
	streamID := uuid.New()
	text := strings.Repeat("compressible ", 20)

	stream, err := notebooks.Create(ctx, streamID, &noted{Text: text})
	harness.RequireNoError(t, err, "create")
	for range 3 {
		stream, err = notebooks.Append(ctx, stream, &noted{Text: text})
		harness.RequireNoError(t, err, "append")
	}

	var (
		eventType   string
		compression string
		metadata    []byte
	)
	err = rig.OwnerPool().QueryRow(ctx, `SELECT event_type, data_compression, metadata
        FROM events WHERE stream_id = $1 AND version = 0`, streamID.String()).Scan(&eventType,
		&compression, &metadata)
	harness.RequireNoError(t, err, "read the state row")
	if eventType != "Noted" || compression != "zstd" || len(metadata) != 8 ||
		binary.BigEndian.Uint32(metadata[:4]) != 4 || binary.BigEndian.Uint32(metadata[4:]) != 3 {
		t.Fatalf("state row: type %q, compression %s, metadata %x; want Noted, zstd, "+
			"version 4 and state version 3", eventType, compression, metadata)
	}
}
