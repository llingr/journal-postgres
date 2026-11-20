// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model"
	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/services"
)

type openAccountRequest struct {
	Tier string `json:"tier" binding:"required,oneof=STANDARD PREMIUM PLATINUM"`
}

// OpenAccount opens an account at the tier in the body, answering 201 with the account
func OpenAccount(accountService services.AccountService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request openAccountRequest
		err := c.ShouldBindJSON(&request)
		if err != nil {
			respondWithBadRequest(c, err)
			return
		}

		var account model.Account
		account, err = accountService.Open(c.Request.Context(), request.Tier)
		if err != nil {
			respondWithError(c, err)
			return
		}
		c.JSON(http.StatusCreated, account)
	}
}
