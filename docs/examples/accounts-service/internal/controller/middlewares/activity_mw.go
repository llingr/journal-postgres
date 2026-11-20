// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package middlewares holds the handlers that run before a route's own
package middlewares

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// Activity logs one line per request once it completes, tagged with the route's activity name
func Activity(activity string, logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()

		c.Next()

		fields := []zap.Field{
			zap.String("activity", activity),
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("duration", time.Since(started)),
		}
		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("error", c.Errors.String()))
		}
		logger.Info("request", fields...)
	}
}
