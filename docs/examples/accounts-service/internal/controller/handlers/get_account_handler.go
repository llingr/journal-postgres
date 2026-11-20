// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model/constants"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/services"
)

// GetAccount answers the account as its events leave it
func GetAccount(accountService services.AccountService) gin.HandlerFunc {
	return func(c *gin.Context) {
		accountID := c.MustGet(constants.AccountIdParam).(uuid.UUID)

		account, err := accountService.Account(c.Request.Context(), accountID)
		if err != nil {
			respondWithError(c, err)
			return
		}
		c.JSON(http.StatusOK, account)
	}
}
