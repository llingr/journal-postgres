// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/llingr/journal-postgres/tests/harness"
)

// TestVersionMustNotBeNegative covers contract B1: version 0 is the stream's state, and a
// version below it is refused.
func TestVersionMustNotBeNegative(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()

	err := harness.InsertEvents(ctx, rig.AppPool(), harness.NewEvent(uuid.New(), 0))
	harness.RequireNoError(t, err, "insert the state row, version 0")

	for name, version := range map[string]int32{
		"negative": -1, "minimum_integer": math.MinInt32,
	} {
		t.Run(name, func(t *testing.T) {
			err := harness.InsertEvents(ctx, rig.AppPool(), harness.NewEvent(uuid.New(), version))
			harness.RequireSQLState(t, err, harness.SQLStateCheckViolation,
				harness.ConstraintEventsVersion)
		})
	}
}

// TestRepeatedVersionRefused covers contract B3: the primary key refuses a version the
// stream already has, the head or an earlier one.
func TestRepeatedVersionRefused(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 2)

	for name, version := range map[string]int32{"repeat_head": 2, "repeat_earlier": 1} {
		t.Run(name, func(t *testing.T) {
			err := harness.InsertEvents(ctx, rig.AppPool(), harness.NewEvent(streamID, version))
			harness.RequireSQLState(t, err, harness.SQLStateUniqueViolation,
				harness.ConstraintEventsPrimaryKey)
		})
	}

	t.Run("repeat_in_one_statement", func(t *testing.T) {
		otherStreamID := uuid.New()
		err := harness.InsertEvents(ctx, rig.AppPool(),
			harness.NewEvent(otherStreamID, 1), harness.NewEvent(otherStreamID, 1))
		harness.RequireSQLState(t, err, harness.SQLStateUniqueViolation,
			harness.ConstraintEventsPrimaryKey)
	})
}

// TestAVersionPastTheHeadIsRefused covers contract B8: whoever writes, a version is refused
// unless the stream stores the one before it, which no writer can step around, whether the
// stream is unwritten, the gap is after the head, or inside one statement.
func TestAVersionPastTheHeadIsRefused(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 2)

	for name, attempt := range map[string][]harness.Event{
		"unwritten_stream_from_2": {harness.NewEvent(uuid.New(), 2)},
		"past_the_head":           {harness.NewEvent(streamID, 4)},
		"gap_in_one_statement":    {harness.NewEvent(streamID, 3), harness.NewEvent(streamID, 5)},
	} {
		t.Run(name, func(t *testing.T) {
			err := harness.InsertEvents(ctx, rig.AppPool(), attempt...)
			harness.RequireSQLState(t, err, harness.SQLStateCheckViolation,
				harness.ConstraintEventsContiguous)
		})
	}
	stored, err := harness.ReadStream(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the stream back")
	requireVersionsFromOne(t, stored, 2)
}

// TestMultiRowInsertInOneStatement covers contract B4.
func TestMultiRowInsertInOneStatement(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := uuid.New()

	err := harness.InsertEvents(ctx, rig.AppPool(),
		harness.NewEvent(streamID, 1), harness.NewEvent(streamID, 2), harness.NewEvent(streamID, 3))
	harness.RequireNoError(t, err, "append versions 1, 2 and 3 in one statement")

	stored, err := harness.ReadStream(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the stream back")
	requireVersionsFromOne(t, stored, 3)
}

// TestInterleavedStreamsInOneStatement covers contract B6.
func TestInterleavedStreamsInOneStatement(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	firstStreamID := uuid.New()
	secondStreamID := uuid.New()

	err := harness.InsertEvents(ctx, rig.AppPool(),
		harness.NewEvent(firstStreamID, 1),
		harness.NewEvent(secondStreamID, 1),
		harness.NewEvent(firstStreamID, 2),
		harness.NewEvent(secondStreamID, 2),
		harness.NewEvent(firstStreamID, 3),
		harness.NewEvent(secondStreamID, 3))
	harness.RequireNoError(t, err, "append two interleaved streams in one statement")

	for _, streamID := range []uuid.UUID{firstStreamID, secondStreamID} {
		stored, err := harness.ReadStream(ctx, rig.AppPool(), streamID)
		harness.RequireNoError(t, err, "read stream back")
		requireVersionsFromOne(t, stored, 3)
	}
}

// TestConcurrentAppendsOfTheSameVersion covers contract B7. Both transactions
// are open before either inserts, so the losing session blocks on the primary
// key index until the winner commits.
func TestConcurrentAppendsOfTheSameVersion(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := uuid.New()

	harness.RequireNoError(t, harness.InsertEvents(ctx, rig.AppPool(), harness.NewEvent(streamID, 1)),
		"append version 1")

	firstPool := rig.OpenExtraAppPool()
	secondPool := rig.OpenExtraAppPool()

	firstTransaction, err := firstPool.Begin(ctx)
	harness.RequireNoError(t, err, "begin the first transaction")
	secondTransaction, err := secondPool.Begin(ctx)
	harness.RequireNoError(t, err, "begin the second transaction")

	// Both transactions are open here; the barrier releases them together.
	barrier := make(chan struct{})
	results := make([]error, 2)
	var waitGroup sync.WaitGroup

	appendAndCommit := func(index int, transaction pgx.Tx) {
		defer waitGroup.Done()
		<-barrier
		insertError := harness.InsertEvents(ctx, transaction, harness.NewEvent(streamID, 2))
		if insertError != nil {
			_ = transaction.Rollback(ctx)
			results[index] = insertError
			return
		}
		results[index] = transaction.Commit(ctx)
	}

	waitGroup.Add(2)
	go appendAndCommit(0, firstTransaction)
	go appendAndCommit(1, secondTransaction)
	close(barrier)
	waitGroup.Wait()

	winners := 0
	for index, result := range results {
		if result == nil {
			winners++
			continue
		}
		if errors.Is(result, pgx.ErrTxClosed) {
			t.Fatalf("session %d reported a closed transaction: %v", index, result)
		}
		harness.RequireSQLState(t, result, harness.SQLStateUniqueViolation,
			harness.ConstraintEventsPrimaryKey)
	}
	if winners != 1 {
		t.Fatalf("%d of 2 concurrent appends committed, want exactly 1: %v", winners, results)
	}

	versions, err := harness.StreamVersions(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read versions after the race")
	if len(versions) != 2 || versions[0] != 1 || versions[1] != 2 {
		t.Fatalf("stream holds %v after the race, want [1 2]", versions)
	}
}
