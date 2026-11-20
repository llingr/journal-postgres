// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"

	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/tests/harness"
)

// TestTheReadmesDeleteFreesARefusedVersionsStream: the operator's DELETE the README gives for
// a version the destination refuses for good, run as the owner, lets the next append publish
// the rest, the event staying as the record
func TestTheReadmesDeleteFreesARefusedVersionsStream(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	publisher := &refusingPublisher{
		refused: 1,
	}
	failures := 0
	repository := journal.NewRepository[struct{}](journal.NewStore(
		postgres.NewBackend(rig.AppPool()),
		journal.WithPublisher(publisher),
		journal.WithHooks(publish.Hooks{
			Failed: func(uuid.UUID, error) {
				failures++
			},
		})))
	streamID := uuid.New()

	stream, err := repository.Create(ctx, streamID, &noted{Text: "refused"})
	harness.RequireNoError(t, err, "create")
	stream, err = repository.Append(ctx, stream, &noted{Text: "held back"})
	harness.RequireNoError(t, err, "append version 2")
	if failures != 2 || len(publisher.versions()) != 0 {
		t.Fatalf("with version 1 refused: %d failures, published %v; want 2 and none",
			failures, publisher.versions())
	}

	_, err = rig.OwnerPool().Exec(ctx,
		`DELETE FROM events_outbox WHERE stream_id = $1 AND version = $2`, streamID.String(), 1)
	harness.RequireNoError(t, err, "delete the refused version's outbox row")
	_, err = repository.Append(ctx, stream, &noted{Text: "released"})
	harness.RequireNoError(t, err, "append version 3")
	if !slices.Equal(publisher.versions(), []int32{2, 3}) {
		t.Fatalf("published after the delete: %v, want versions 2 and 3", publisher.versions())
	}
	requireRows(t, rig, streamID, 3, 0)
}

// refusingPublisher refuses one version every time and records the rest
type refusingPublisher struct {
	recordingPublisher
	refused int32
}

func (p *refusingPublisher) PublishAll(ctx context.Context, partitionKey string,
	toPublish []streams.Event) (int, error) {
	for index, event := range toPublish {
		if event.Version == p.refused {
			durable, err := p.recordingPublisher.PublishAll(ctx, partitionKey, toPublish[:index])
			return durable, errors.Join(err, errors.New("record too large"))
		}
	}
	return p.recordingPublisher.PublishAll(ctx, partitionKey, toPublish)
}
