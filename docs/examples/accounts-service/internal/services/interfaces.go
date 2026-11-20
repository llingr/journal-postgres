// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package services declares the service-layer interfaces; the business features of the
// application are listed in this one file.
package services

import (
	"context"

	"github.com/google/uuid"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model"
)

// AccountService opens accounts and changes them by the events their rules allow
type AccountService interface {
	// Open starts a new account at a tier
	Open(ctx context.Context, tier string) (model.Account, error)

	// Account reads an account as its events leave it; model.ErrAccountNotFound for none
	Account(ctx context.Context, accountID uuid.UUID) (model.Account, error)

	// Upgrade moves an open account to a new tier; model.ErrAccountClosed once it is closed
	Upgrade(ctx context.Context, accountID uuid.UUID, tier string) (model.Account, error)

	// Close ends an open account for a reason; model.ErrAccountClosed when it is already closed
	Close(ctx context.Context, accountID uuid.UUID, reason string) (model.Account, error)
}
