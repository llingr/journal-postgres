// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"testing"

	"github.com/llingr/journal/journaltest"
	"github.com/llingr/journal/persistence"

	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/tests/harness"
)

// TestTheLibraryOverPostgres runs everything the library promises an application, each test
// in a schema of its own
func TestTheLibraryOverPostgres(t *testing.T) {
	journaltest.RunSuite(t, func(t *testing.T) persistence.Backend {
		return postgres.NewBackend(harness.New(t).AppPool())
	})
}
