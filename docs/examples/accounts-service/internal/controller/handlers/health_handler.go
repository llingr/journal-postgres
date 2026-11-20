// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Health answers 200 once the service is wired, which is after the schema has migrated, and
// 503 from the moment it starts stopping, so a readiness probe takes it out of service
func Health(stopping func() bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if stopping() {
			c.Status(http.StatusServiceUnavailable)
			return
		}
		c.Status(http.StatusOK)
	}
}
