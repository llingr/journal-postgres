// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package harness

import (
	"context"
	"io/fs"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/llingr/llingr-pgx/roles"
	pgxschema "github.com/llingr/llingr-pgx/schema"

	"github.com/llingr/journal-postgres/schema"
)

// SchemaDirectoryVariable overrides the directory the migrations are read from,
// which is how a scratch copy of the schema is put under test.
const SchemaDirectoryVariable = "EVENT_STORE_SCHEMA_DIR"

// MigrationRoles maps the placeholders used in the migration files onto the
// usernames the suite creates.
func MigrationRoles() roles.PlaceholderUsernames {
	return roles.NewPlaceholderBuilder().
		WithOwner(OwnerUsername).
		WithApp(AppUsername).
		MustBuild()
}

// schemaFiles is the embedded schema, or the directory SchemaDirectoryVariable names
func schemaFiles() fs.FS {
	if directory := os.Getenv(SchemaDirectoryVariable); directory != "" {
		return os.DirFS(directory)
	}
	return schema.FS
}

// applyMigrations runs every up-migration through llingr-pgx against the pool's
// search path
func applyMigrations(ctx context.Context, ownerPool *pgxpool.Pool) error {
	return pgxschema.Migrate(ctx, ownerPool, schemaFiles(), MigrationRoles(),
		pgxschema.WithMigrationsTable(schema.MigrationsTable))
}
