// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model/constants"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/services"
)

type upgradeAccountRequest struct {
	Tier string `json:"tier" binding:"required,oneof=STANDARD PREMIUM PLATINUM"`
}

// UpgradeAccount moves an open account to the tier in the body
func UpgradeAccount(accountService services.AccountService) gin.HandlerFunc {
	return func(c *gin.Context) {
		accountID := c.MustGet(constants.AccountIdParam).(uuid.UUID)

		var request upgradeAccountRequest
		err := c.ShouldBindJSON(&request)
		if err != nil {
			respondWithBadRequest(c, err)
			return
		}

		var account model.Account
		account, err = accountService.Upgrade(c.Request.Context(), accountID, request.Tier)
		if err != nil {
			respondWithError(c, err)
			return
		}
		c.JSON(http.StatusOK, account)
	}
}
