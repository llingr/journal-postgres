// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/streams"

	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/tests/harness"
)

// TestEveryCallWithoutItsPrivilegeIsNotPermitted: a role stripped of its grants gets
// ErrNotPermitted from every call, reads included, rather than an empty answer or a driver
// error a caller has to pick apart
func TestEveryCallWithoutItsPrivilegeIsNotPermitted(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := uuid.New()
	appendStoreVersions(t, postgres.NewBackend(rig.AppPool()), streamID, 1)
	for _, statement := range []string{
		`REVOKE ALL ON events FROM ` + harness.AppUsername,
		`REVOKE ALL ON events_outbox FROM ` + harness.AppUsername,
	} {
		_, err := rig.OwnerPool().Exec(ctx, statement)
		harness.RequireNoError(t, err, statement)
	}
	store := postgres.NewBackend(rig.AppPool())

	for name, call := range map[string]func() error{
		"Append": func() error {
			_, err := store.Append(ctx, appendRequest(streamID, 2))
			return err
		},
		"SaveState": func() error {
			_, err := store.SaveState(ctx, persistence.PersistedState{
				StreamID:        streamID,
				Version:         1,
				StateVersion:    1,
				LastEventType:   "OrderPlaced",
				LastAppendTs:    harness.DefaultAppendTs,
				StateData:       []byte(`{}`),
				DataFormat:      "json",
				DataCompression: "none",
				DataEncryption:  "none",
			})
			return err
		},
		"QueryStream": func() error {
			_, err := store.QueryStream(ctx, streamID, 1, 10)
			return err
		},
		"QueryStreamRange": func() error {
			_, err := store.QueryStreamRange(ctx, streamID, 1, 1)
			return err
		},
		"QueryHead": func() error {
			_, err := store.QueryHead(ctx, streamID)
			return err
		},
		"QueryState": func() error {
			_, _, err := store.QueryState(ctx, streamID)
			return err
		},
		"QueryPending": func() error {
			_, err := store.QueryPending(ctx, streamID)
			return err
		},
		"QueryPendingStreams": func() error {
			_, err := store.QueryPendingStreams(ctx, time.Time{}, time.Now(), 10)
			return err
		},
		"DeletePublished": func() error {
			return store.DeletePublished(ctx, streamID, 1)
		},
	} {
		if err := call(); !errors.Is(err, streams.ErrNotPermitted) {
			t.Errorf("%s without its privilege: got %v, want ErrNotPermitted", name, err)
		}
	}
}

// TestAStateRowWithDamagedVersionsFailsTheRead: a state row whose metadata is not the two
// versions the backend writes, as a writer of its own could leave it, fails the read rather
// than be taken as a state at some version
func TestAStateRowWithDamagedVersionsFailsTheRead(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	notes := newNotes(journal.NewStore(postgres.NewBackend(rig.AppPool())))
	streamID := uuid.New()
	_, err := notes.Create(ctx, streamID, &noted{Text: "first"})
	harness.RequireNoError(t, err, "create")
	_, err = rig.OwnerPool().Exec(ctx, `INSERT INTO events (stream_id, version, data_format,
        data_compression, data_encryption, event_append_ts, event_data, event_type,
        data_encryption_kid, metadata, partition_key)
        VALUES ($1, 0, 'json', 'none', 'none', now(), '\x7b7d', 'Noted', '', '\x000001', '')`,
		streamID.String())
	harness.RequireNoError(t, err, "write a state row with three bytes of metadata")

	_, _, err = postgres.NewBackend(rig.AppPool()).QueryState(ctx, streamID)
	if err == nil {
		t.Fatal("read of a state row with damaged versions succeeded, want an error")
	}
}

// TestNewBackendWithoutAPoolPanics: a backend with nowhere to write is a wiring mistake,
// stopped where it is built
func TestNewBackendWithoutAPoolPanics(t *testing.T) {
	defer func() {
		recovered := recover()
		err, isError := recovered.(error)
		if !isError || !errors.Is(err, postgres.ErrNoPool) {
			t.Fatalf("NewBackend(nil) recovered %v, want ErrNoPool", recovered)
		}
	}()
	postgres.NewBackend(nil)
}
