// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Command quickstart is journal's quick start: it migrates the schema, then creates, appends to
// and reads one order, publishing each event to the log
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/llingr/journal"
	postgres "github.com/llingr/journal-postgres"
	"github.com/llingr/journal-postgres/schema"
	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/streams"
	"github.com/llingr/llingr-pgx/roles"
	pgxschema "github.com/llingr/llingr-pgx/schema"
)

// Order is the state an order's events fold to
type Order struct {
	Amount  int
	Shipped bool
}

// OrderPlaced is the event that starts an order
type OrderPlaced struct {
	Amount int `json:"amount"`
}

func (e *OrderPlaced) EventType() string {
	return "ORDER_PLACED"
}

func (e *OrderPlaced) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (e *OrderPlaced) MarshalPayload() ([]byte, error) {
	return json.Marshal(e)
}

func (e *OrderPlaced) UnmarshalPayload(encoded []byte) error {
	return json.Unmarshal(encoded, e)
}

// OrderShipped is the event that ships an order
type OrderShipped struct{}

func (e *OrderShipped) EventType() string {
	return "ORDER_SHIPPED"
}

func (e *OrderShipped) DataFormat() codec.DataFormat {
	return codec.JSON
}

func (e *OrderShipped) MarshalPayload() ([]byte, error) {
	return json.Marshal(e)
}

func (e *OrderShipped) UnmarshalPayload(encoded []byte) error {
	return json.Unmarshal(encoded, e)
}

// logPublisher publishes to the log, where a service would produce to Kafka
type logPublisher struct{}

func (logPublisher) PublishAll(_ context.Context, partitionKey string,
	events []streams.Event) (int, error) {
	for _, event := range events {
		log.Printf("publish %s version %d under %s: %s", event.EventType, event.Version,
			partitionKey, event.Payload)
	}
	return len(events), nil
}

func main() {
	ctx := context.Background()

	// the owner role applies the schema; the app role is the one the application connects as
	ownerPool, err := pgxpool.New(ctx, os.Getenv("OWNER_DATABASE_URL"))
	check(err)
	err = pgxschema.Migrate(ctx, ownerPool, schema.FS,
		roles.NewPlaceholderBuilder().WithOwner("journal_owner").WithApp("journal_app").MustBuild(),
		pgxschema.WithMigrationsTable(schema.MigrationsTable))
	check(err)
	ownerPool.Close()
	appPool, err := pgxpool.New(ctx, os.Getenv("APP_DATABASE_URL"))
	check(err)
	defer appPool.Close()

	eventStore := journal.NewStore(postgres.NewBackend(appPool),
		journal.WithPublisher(logPublisher{}))
	eventStore.Start(ctx)
	defer func() {
		check(eventStore.Shutdown(ctx))
	}()

	orders := journal.NewRepository(eventStore, journal.WithTransitions(
		journal.Transition(func(order Order, placed *OrderPlaced) (Order, error) {
			order.Amount = placed.Amount
			return order, nil
		}),
		journal.Transition(func(order Order, _ *OrderShipped) (Order, error) {
			if order.Shipped {
				return order, errors.New("the order has shipped already")
			}
			order.Shipped = true
			return order, nil
		}),
	))

	orderID := uuid.New()
	order, err := orders.Create(ctx, orderID, &OrderPlaced{Amount: 120})
	check(err)
	_, err = orders.Append(ctx, order, &OrderShipped{})
	check(err)
	order, err = orders.Get(ctx, orderID)
	check(err)
	fmt.Printf("order %s: %+v at version %d\n", orderID, order.State(), order.Version())
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
