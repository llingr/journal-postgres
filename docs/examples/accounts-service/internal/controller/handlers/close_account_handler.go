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

type closeAccountRequest struct {
	Reason string `json:"reason" binding:"required"`
}

// CloseAccount closes an open account for the reason in the body
func CloseAccount(accountService services.AccountService) gin.HandlerFunc {
	return func(c *gin.Context) {
		accountID := c.MustGet(constants.AccountIdParam).(uuid.UUID)

		var request closeAccountRequest
		err := c.ShouldBindJSON(&request)
		if err != nil {
			respondWithBadRequest(c, err)
			return
		}

		var account model.Account
		account, err = accountService.Close(c.Request.Context(), accountID, request.Reason)
		if err != nil {
			respondWithError(c, err)
			return
		}
		c.JSON(http.StatusOK, account)
	}
}
