// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// NoRoute answers a bare 404. It writes the header now, since gin adds its own text body to a
// no-route response that has not been written yet.
func NoRoute() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.AbortWithStatus(http.StatusNotFound)
	}
}
