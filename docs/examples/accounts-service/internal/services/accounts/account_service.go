// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package accounts is the account service over the accounts repo
package accounts

import (
	"context"

	"github.com/google/uuid"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/repos"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/services"
)

type accountService struct {
	accountsRepo repos.AccountsRepo
}

// New returns the account service, keeping accounts in the repo
func New(accountsRepo repos.AccountsRepo) services.AccountService {
	return &accountService{
		accountsRepo: accountsRepo,
	}
}

func (a *accountService) Open(ctx context.Context, tier string) (model.Account, error) {
	return a.accountsRepo.Open(ctx, &model.AccountOpened{
		AccountID: uuid.New(),
		Tier:      tier,
	})
}

func (a *accountService) Account(ctx context.Context, accountID uuid.UUID) (model.Account,
	error) {
	return a.accountsRepo.Get(ctx, accountID)
}

func (a *accountService) Upgrade(ctx context.Context, accountID uuid.UUID,
	tier string) (model.Account, error) {
	return a.accountsRepo.Append(ctx, accountID, &model.AccountUpgraded{
		Tier: tier,
	})
}

func (a *accountService) Close(ctx context.Context, accountID uuid.UUID,
	reason string) (model.Account, error) {
	return a.accountsRepo.Append(ctx, accountID, &model.AccountClosed{
		Reason: reason,
	})
}
