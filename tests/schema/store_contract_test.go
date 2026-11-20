// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"testing"

	"github.com/llingr/journal/persistence"
	"github.com/llingr/journal/persistence/backendtest"

	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/tests/harness"
)

// TestPostgresStoreContract runs the store contract against Postgres.
//
// The factory is where the isolation lives: each subtest gets its own schema
// with the migrations applied, and harness.New registers the teardown against
// that subtest, so no state is shared and a missing database is reported by
// the subtest that wanted it.
func TestPostgresStoreContract(t *testing.T) {
	backendtest.RunContract(t, func(t *testing.T) persistence.Backend {
		return postgres.NewBackend(harness.New(t).AppPool())
	})
}
