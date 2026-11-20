// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package handlers answers each route
package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/llingr/journal-postgres/docs/examples/accounts-service/internal/model"
)

// respondWithError answers the status an account error means, and keeps the error for the
// activity log
func respondWithError(c *gin.Context, err error) {
	_ = c.Error(err)
	switch {
	case errors.Is(err, model.ErrAccountNotFound):
		c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
			"error": model.ErrAccountNotFound.Error(),
		})
	case errors.Is(err, model.ErrAccountClosed):
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": model.ErrAccountClosed.Error(),
		})
	case errors.Is(err, model.ErrAccountChanged):
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{
			"error": model.ErrAccountChanged.Error(),
		})
	default:
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"error": "internal error",
		})
	}
}

// respondWithBadRequest answers a request body that does not bind
func respondWithBadRequest(c *gin.Context, err error) {
	c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
		"error": err.Error(),
	})
}
