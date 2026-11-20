// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package model is what the service looks after: accounts, the events that change them and
// the rules those events obey
package model

import (
	"errors"

	"github.com/google/uuid"
)

// Account is what an account's events fold to
type Account struct {
	AccountID uuid.UUID `json:"accountId"`
	Tier      string    `json:"tier"`
	Closed    bool      `json:"closed"`
	Version   int32     `json:"version"`
}

// ErrAccountNotFound is an account with no events
var ErrAccountNotFound = errors.New("no such account")

// ErrAccountClosed is a change to an account that has been closed
var ErrAccountClosed = errors.New("the account is closed")

// ErrAccountChanged is a change that lost the race with another change to the same account
var ErrAccountChanged = errors.New("the account changed at the same time")

// WithOpened is the account an AccountOpened starts
func (a Account) WithOpened(opened *AccountOpened) (Account, error) {
	a.AccountID = opened.AccountID
	a.Tier = opened.Tier
	return a, nil
}

// WithUpgraded moves an open account to a new tier
func (a Account) WithUpgraded(upgraded *AccountUpgraded) (Account, error) {
	if a.Closed {
		return a, ErrAccountClosed
	}
	a.Tier = upgraded.Tier
	return a, nil
}

// WithClosed closes an open account
func (a Account) WithClosed(_ *AccountClosed) (Account, error) {
	if a.Closed {
		return a, ErrAccountClosed
	}
	a.Closed = true
	return a, nil
}
