// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package schema is the event store's migration files, and the version table to record them in
package schema

import "embed"

// FS holds the migration files at its root
//
//go:embed *.sql
var FS embed.FS

// MigrationsTable is the table to record the applied version of FS in, apart from the
// application's own migrations, whose version would otherwise share the default table and
// skip or collide with these
const MigrationsTable = "journal_schema_migrations"
