// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package middlewares

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// UUIDParam parses the path variable as a UUID and keeps it in the context under the same
// name, answering 400 when it is not one
func UUIDParam(name string) gin.HandlerFunc {
	return func(c *gin.Context) {
		value, err := uuid.Parse(c.Param(name))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error": name + " is not a UUID",
			})
			return
		}
		c.Set(name, value)
		c.Next()
	}
}
