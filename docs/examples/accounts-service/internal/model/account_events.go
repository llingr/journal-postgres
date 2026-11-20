// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"github.com/google/uuid"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/formatjson"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model/constants"
)

// AccountOpened starts an account's stream
type AccountOpened struct {
	AccountID uuid.UUID `json:"accountId"`
	Tier      string    `json:"tier"`
}

func (a *AccountOpened) EventType() string {
	return constants.AccountOpenedEventType
}

func (a *AccountOpened) DataFormat() codec.DataFormat {
	return formatjson.Format
}

func (a *AccountOpened) MarshalPayload() ([]byte, error) {
	return formatjson.Marshal(a)
}

func (a *AccountOpened) UnmarshalPayload(encoded []byte) error {
	return formatjson.Unmarshal(encoded, a)
}

// AccountUpgraded moves an account to a new tier
type AccountUpgraded struct {
	Tier string `json:"tier"`
}

func (a *AccountUpgraded) EventType() string {
	return constants.AccountUpgradedEventType
}

func (a *AccountUpgraded) DataFormat() codec.DataFormat {
	return formatjson.Format
}

func (a *AccountUpgraded) MarshalPayload() ([]byte, error) {
	return formatjson.Marshal(a)
}

func (a *AccountUpgraded) UnmarshalPayload(encoded []byte) error {
	return formatjson.Unmarshal(encoded, a)
}

// AccountClosed ends an account; nothing changes it afterwards
type AccountClosed struct {
	Reason string `json:"reason"`
}

func (a *AccountClosed) EventType() string {
	return constants.AccountClosedEventType
}

func (a *AccountClosed) DataFormat() codec.DataFormat {
	return formatjson.Format
}

func (a *AccountClosed) MarshalPayload() ([]byte, error) {
	return formatjson.Marshal(a)
}

func (a *AccountClosed) UnmarshalPayload(encoded []byte) error {
	return formatjson.Unmarshal(encoded, a)
}
