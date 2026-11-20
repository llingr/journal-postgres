// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"context"
	"testing"

	"github.com/llingr/journal-postgres/tests/harness"
)

// TestTrimmingKeepsTheTail covers contract F1.
func TestTrimmingKeepsTheTail(t *testing.T) {
	rig := harness.New(t)
	ctx := context.Background()
	streamID := appendStream(t, rig, 5)

	commandTag, err := rig.OwnerPool().Exec(ctx,
		`DELETE FROM events WHERE stream_id = $1 AND version < 3`, streamID.String())
	harness.RequireNoError(t, err, "trim the prefix below version 3")
	if commandTag.RowsAffected() != 2 {
		t.Fatalf("trim removed %d rows, want 2", commandTag.RowsAffected())
	}

	versions, err := harness.StreamVersions(ctx, rig.AppPool(), streamID)
	harness.RequireNoError(t, err, "read the surviving versions")
	if len(versions) != 3 || versions[0] != 3 {
		t.Fatalf("stream holds %v after the trim, want [3 4 5]", versions)
	}

}
