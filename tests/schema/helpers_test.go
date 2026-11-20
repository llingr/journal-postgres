// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/formatjson"
	"github.com/llingr/journal/streams"

	"github.com/llingr/journal-postgres/tests/harness"
)

// appendStream writes versions 1 to count and returns the stream identifier.
func appendStream(t *testing.T, rig *harness.Harness, count int32) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	streamID := uuid.New()

	events := make([]harness.Event, 0, count)
	for version := int32(1); version <= count; version++ {
		events = append(events, harness.NewEvent(streamID, version))
	}
	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), events...), "append the stream")
	return streamID
}

// noted is an event with text worth compressing
type noted struct {
	Text string `json:"text"`
}

func (n *noted) EventType() string {
	return "Noted"
}

func (n *noted) DataFormat() codec.DataFormat {
	return formatjson.Format
}

func (n *noted) MarshalPayload() ([]byte, error) {
	return formatjson.Marshal(n)
}

func (n *noted) UnmarshalPayload(encoded []byte) error {
	return formatjson.Unmarshal(encoded, n)
}

// recordingPublisher records the version of every event it is sent, from any goroutine
type recordingPublisher struct {
	mutex     sync.Mutex
	published []int32
}

func (p *recordingPublisher) PublishAll(_ context.Context, _ string,
	toPublish []streams.Event) (int, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for _, event := range toPublish {
		p.published = append(p.published, event.Version)
	}
	return len(toPublish), nil
}

func (p *recordingPublisher) versions() []int32 {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return slices.Clone(p.published)
}

// requireVersionsFromOne fails unless the stored stream is versions 1 to count, in order.
func requireVersionsFromOne(t *testing.T, stored []harness.StoredEvent, count int) {
	t.Helper()
	if len(stored) != count {
		t.Fatalf("stream holds %d events, want %d", len(stored), count)
	}
	for index, event := range stored {
		if event.Version != int32(index+1) {
			t.Fatalf("event %d is version %d, want %d", index, event.Version, index+1)
		}
	}
}
