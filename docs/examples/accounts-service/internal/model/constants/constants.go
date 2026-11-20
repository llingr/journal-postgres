// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package constants

// ApplicationName names the service in its logs and its Postgres sessions
const ApplicationName = "accounts-service"

// The account event types, as stored in each row's event_type
const (
	AccountOpenedEventType   = "ACCOUNT_OPENED"
	AccountUpgradedEventType = "ACCOUNT_UPGRADED"
	AccountClosedEventType   = "ACCOUNT_CLOSED"
)

// AccountIdParam is the path variable naming an account
const AccountIdParam = "account_id"
