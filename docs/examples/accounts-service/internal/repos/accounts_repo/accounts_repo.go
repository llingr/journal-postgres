// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package accounts_repo keeps accounts in the journal, one stream per account
package accounts_repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/llingr/journal"
	"github.com/llingr/journal/streams"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/repos"
)

type accountsRepo struct {
	accounts *streams.Repository[model.Account]
}

// New returns the accounts repo over the event store, each event folded by the account's own
// rule for it
func New(eventStore *journal.Store) repos.AccountsRepo {
	return &accountsRepo{
		accounts: journal.NewRepository(eventStore, journal.WithTransitions(
			journal.Transition(model.Account.WithOpened),
			journal.Transition(model.Account.WithUpgraded),
			journal.Transition(model.Account.WithClosed),
		)),
	}
}

func (a *accountsRepo) Open(ctx context.Context, opened *model.AccountOpened) (model.Account,
	error) {
	stored, err := a.accounts.Create(ctx, opened.AccountID, opened)
	if err != nil {
		return model.Account{}, fmt.Errorf("open account %s: %w", opened.AccountID, err)
	}
	return accountOf(stored), nil
}

func (a *accountsRepo) Get(ctx context.Context, accountID uuid.UUID) (model.Account, error) {
	stored, err := a.accounts.Get(ctx, accountID)
	if err != nil {
		return model.Account{}, fmt.Errorf("get account %s: %w", accountID, err)
	}
	if stored.Version() == 0 {
		return model.Account{}, fmt.Errorf("get account %s: %w", accountID, model.ErrAccountNotFound)
	}
	return accountOf(stored), nil
}

func (a *accountsRepo) Append(ctx context.Context, accountID uuid.UUID,
	event streams.DomainEvent) (model.Account, error) {
	stored, err := a.accounts.Get(ctx, accountID)
	if err != nil {
		return model.Account{}, fmt.Errorf("change account %s: %w", accountID, err)
	}
	if stored.Version() == 0 {
		return model.Account{}, fmt.Errorf("change account %s: %w", accountID,
			model.ErrAccountNotFound)
	}

	stored, err = a.accounts.Append(ctx, stored, event)
	if errors.Is(err, streams.ErrVersionConflict) {
		return model.Account{}, fmt.Errorf("change account %s: %w: %w", accountID,
			model.ErrAccountChanged, err)
	}
	if err != nil {
		return model.Account{}, fmt.Errorf("change account %s: %w", accountID, err)
	}
	return accountOf(stored), nil
}

// accountOf is the account as stored: its folded state at its stream's version
func accountOf(stored streams.Stream[model.Account]) model.Account {
	account := stored.State()
	account.Version = stored.Version()
	return account
}
