// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package controller is the service's HTTP interface
package controller

import (
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/controller/handlers"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/controller/middlewares"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model/constants"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/services"
)

// RegisterHandlers adds every route to the HTTP web server
func RegisterHandlers(engine *gin.Engine, accountService services.AccountService,
	stopping func() bool, logger *zap.Logger) {
	const (
		healthPath   = "/health"
		basePath     = "/v1"
		accountsPath = basePath + "/accounts"
		accountPath  = accountsPath + "/:" + constants.AccountIdParam
		upgradePath  = accountPath + "/upgrades"
		closurePath  = accountPath + "/closure"
	)

	// reads and validates the :account_id path variable
	accountIdParam := middlewares.UUIDParam(constants.AccountIdParam)

	engine.Use(gin.Recovery())

	engine.GET(healthPath,
		handlers.Health(stopping),
	)
	engine.POST(accountsPath,
		middlewares.Activity("OPEN_ACCOUNT", logger),
		handlers.OpenAccount(accountService),
	)
	engine.GET(accountPath,
		middlewares.Activity("GET_ACCOUNT", logger),
		accountIdParam,
		handlers.GetAccount(accountService),
	)
	engine.POST(upgradePath,
		middlewares.Activity("UPGRADE_ACCOUNT", logger),
		accountIdParam,
		handlers.UpgradeAccount(accountService),
	)
	engine.POST(closurePath,
		middlewares.Activity("CLOSE_ACCOUNT", logger),
		accountIdParam,
		handlers.CloseAccount(accountService),
	)
	engine.NoRoute(
		middlewares.Activity("NO_ROUTE", logger),
		handlers.NoRoute(),
	)
}
