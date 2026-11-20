// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
	"github.com/llingr/journal/persistence"

	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/tests/harness"
)

// TestTimesReadBackInUTC: the append times of events and of the stored state read back in UTC,
// as journal writes them, over a pool built with pgx's defaults, which would hand them back in
// the process's time zone, so a publisher sees one offset whichever path read the event
func TestTimesReadBackInUTC(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	store := postgres.NewBackend(rig.AppPool())
	streamID := uuid.New()

	request := appendRequest(streamID, 1, 2)
	request.State = &persistence.PersistedState{
		StreamID:        streamID,
		Version:         2,
		StateVersion:    1,
		LastEventType:   request.Events[1].EventType,
		LastAppendTs:    request.AppendTs,
		StateData:       []byte(`{}`),
		DataFormat:      codec.JSON,
		DataCompression: compress.None,
		DataEncryption:  encrypt.None,
	}
	_, err := store.Append(ctx, request)
	harness.RequireNoError(t, err, "append two events and their state")

	streamed, err := store.QueryStream(ctx, streamID, 1, 10)
	harness.RequireNoError(t, err, "read the stream")
	ranged, err := store.QueryStreamRange(ctx, streamID, 1, 2)
	harness.RequireNoError(t, err, "read the range")
	state, _, err := store.QueryState(ctx, streamID)
	harness.RequireNoError(t, err, "read the state")

	read := map[string]time.Time{
		"the state's last append": state.LastAppendTs,
	}
	for _, event := range streamed {
		read[fmt.Sprintf("QueryStream version %d", event.Version)] = event.EventAppendTs
	}
	for _, event := range ranged {
		read[fmt.Sprintf("QueryStreamRange version %d", event.Version)] = event.EventAppendTs
	}
	if len(read) != 5 {
		t.Fatalf("read %d times, want 5: two events each way and the state", len(read))
	}
	for what, appended := range read {
		if appended.Location() != time.UTC || !appended.Equal(request.AppendTs) {
			t.Errorf("%s read back as %s in %q, want %s in UTC", what, appended,
				appended.Location(), request.AppendTs)
		}
	}
}
