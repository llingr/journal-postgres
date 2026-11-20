# journal-postgres

The Postgres backend for [journal](https://github.com/llingr/journal), the Go event-sourcing
library with a transactional outbox. It keeps each stream's events, their outbox rows and the
stream's stored state in two tables, written through a pgx pool, and it is tested against
Postgres 18.

## Install

```sh
go get github.com/llingr/journal
go get github.com/llingr/journal-postgres
```

It needs Go 1.26 or later.

## Set up the Database

The backend never migrates. Your application applies the schema with its own migrations, as a
role allowed to create tables, and then connects as a role that is not.

**1. Create the two roles and the database**, as a superuser:

```sql
CREATE ROLE journal_owner LOGIN PASSWORD 'owner-password';
CREATE ROLE journal_app LOGIN PASSWORD 'app-password';
CREATE DATABASE orders OWNER journal_owner;
```

The owner applies the schema and owns its tables. The app role is the one your application
connects as: the schema grants it what appending, reading and publishing need, and nothing
more, so it can never alter a table or delete an event.

**2. Apply the schema** as the owner, for example with llingr-pgx:

```go
import (
	"github.com/llingr/journal-postgres/schema"
	"github.com/llingr/llingr-pgx/roles"
	pgxschema "github.com/llingr/llingr-pgx/schema"
)

err := pgxschema.Migrate(ctx, ownerPool, schema.FS,
	roles.NewPlaceholderBuilder().WithOwner("journal_owner").WithApp("journal_app").MustBuild(),
	pgxschema.WithMigrationsTable(schema.MigrationsTable))
```

The files write the app role as the placeholder `:"app"`, which llingr-pgx replaces with its
name. `schema.MigrationsTable`, `journal_schema_migrations`, records which files are applied,
apart from your own migrations; in the default table the two sets of version numbers would
collide. With another migration tool, replace the placeholder yourself and give these files a
version table of their own too.

**3. Connect as the app role** and pass the backend to journal:

```go
import (
	"github.com/llingr/journal"
	postgres "github.com/llingr/journal-postgres"
)

appPool, err := pgxpool.New(ctx, "postgres://journal_app:app-password@localhost:5432/orders")
eventStore := journal.NewStore(postgres.NewBackend(appPool),
	journal.WithPublisher(publisher))
```

From there everything is journal's, and its README covers it.
[`docs/examples/quickstart`](docs/examples/quickstart/main.go) is all three steps in one
program, run with the role URLs from step 1:

```sh
OWNER_DATABASE_URL=postgres://journal_owner:owner-password@localhost:5432/orders \
APP_DATABASE_URL=postgres://journal_app:app-password@localhost:5432/orders \
go run ./docs/examples/quickstart
```

## What the Schema Enforces

The schema holds every writer to the stream's rules, this backend or any other client: an event
is never updated or deleted by the app role, and a version is refused unless the stream stores
the one before it. The backend returns these refusals as journal's errors. A version another
writer took, or one past the stream's last, is `streams.ErrVersionConflict`; a privilege the
role lacks is `streams.ErrNotPermitted`.

### Other Table Names

The schema creates `events` and `events_outbox`. To use other names, such as to keep two
journals in one database, apply `0001_types.up.sql`, which creates the types and trigger
functions every journal in the schema shares. Then create the tables in your own migrations
with the columns, constraints, grants and triggers of `0002_events.up.sql` and
`0003_events_outbox.up.sql`, and name them to the backend:

```go
backend := postgres.NewBackend(appPool,
	postgres.WithEventsTableName("order_events"),
	postgres.WithEventsOutboxTableName("order_events_outbox"),
)
```

Names are lowercase letters, digits and underscores.

## Operations

**An event the publisher always rejects** holds back every later event of its stream. Delete
its outbox row to let the rest through; the event itself stays saved:

```sql
DELETE FROM events_outbox WHERE stream_id = $1 AND version = $2;
```

**To erase a stream**, as the owner:

```sql
BEGIN;
DELETE FROM events_outbox WHERE stream_id = $1;
DELETE FROM events WHERE stream_id = $1;
COMMIT;
```

Deleting a version from the middle of a stream makes every read of it fail with
`streams.ErrVersionMissing` rather than fold part of the stream.

## Tests

```sh
make test   # starts Postgres 18 in Docker and runs every module's tests
make lint
```

The tests run journal's two suites over Postgres, under the default table names and under
others, and the schema's own tests of its constraints, triggers and grants.

## More Examples

- `docs/examples/accounts-service`: a complete HTTP service on journal, this backend, anvil and
  llingr-pgx

## Licence

Apache License, Version 2.0. See LICENSE, COPYRIGHT and AUTHORS.
