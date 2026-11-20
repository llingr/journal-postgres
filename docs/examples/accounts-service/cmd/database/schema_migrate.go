// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package database

import (
	"context"
	"fmt"

	"github.com/llingr/llingr-pgx/roles"
	dbschema "github.com/llingr/llingr-pgx/schema"

	"github.com/llingr/journal-postgres/schema"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/config"
)

// Migrate applies the journal's migration files as the owner, which holds a connection only
// while it migrates
func Migrate(ctx context.Context, pg config.Postgres) error {
	ownerPool, err := ownerConnection(ctx, pg)
	if err != nil {
		return err
	}
	defer ownerPool.Close()

	var rolePlaceholders roles.PlaceholderUsernames
	rolePlaceholders, err = roles.NewPlaceholderBuilder().
		WithOwner(pg.Roles.Owner.Username).
		WithApp(pg.Roles.App.Username).
		Build()
	if err != nil {
		return fmt.Errorf("build migration role placeholders: %w", err)
	}

	err = dbschema.Migrate(ctx, ownerPool, schema.FS, rolePlaceholders,
		dbschema.WithMigrationsTable(schema.MigrationsTable))
	if err != nil {
		return fmt.Errorf("migrate the journal's schema: %w", err)
	}
	return nil
}
