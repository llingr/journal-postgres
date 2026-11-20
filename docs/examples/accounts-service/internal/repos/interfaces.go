// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package repos declares the persistence interfaces the services use
package repos

import (
	"context"

	"github.com/google/uuid"

	"github.com/llingr/journal/streams"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model"
)

// AccountsRepo keeps each account as its stream of events, so its whole history is kept
type AccountsRepo interface {
	// Open starts an account's stream with the event that opens it
	Open(ctx context.Context, opened *model.AccountOpened) (model.Account, error)

	// Get folds the account's events; model.ErrAccountNotFound when it has none
	Get(ctx context.Context, accountID uuid.UUID) (model.Account, error)

	// Append adds an event to the account as it is now: model.ErrAccountNotFound for an
	// account with no events, model.ErrAccountClosed when the account's rules refuse the event,
	// and model.ErrAccountChanged when another change was saved first
	Append(ctx context.Context, accountID uuid.UUID, event streams.DomainEvent) (model.Account,
		error)
}
