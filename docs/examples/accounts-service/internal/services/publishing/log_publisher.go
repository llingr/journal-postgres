// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package publishing sends the journal's committed events on: here to the log, where a
// deployment would produce them to Kafka
package publishing

import (
	"context"

	"go.uber.org/zap"

	"github.com/llingr/journal/publish"
	"github.com/llingr/journal/streams"
)

type logPublisher struct {
	logger *zap.Logger
}

// New returns a publish.Publisher that logs each event in place of a Kafka producer. It logs
// the key, type and version, never the payload, which can hold personal data.
func New(logger *zap.Logger) publish.Publisher {
	return &logPublisher{
		logger: logger,
	}
}

func (l *logPublisher) PublishAll(_ context.Context, partitionKey string,
	events []streams.Event) (int, error) {
	for _, event := range events {
		l.logger.Info("published",
			zap.String("partitionKey", partitionKey),
			zap.String("eventType", event.EventType),
			zap.Int32("version", event.Version))
	}
	return len(events), nil
}
