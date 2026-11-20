// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

// Package config is the service's static settings, read once at startup
package config

import (
	"embed"

	"github.com/llingr/anvil"
	"github.com/llingr/anvil-koanf/conf"
)

//go:embed config.yaml
var configFS embed.FS

type Config struct {
	Server   Server   `yaml:"server"`
	Postgres Postgres `yaml:"postgres"`
}

// NewProvider loads the embedded config.yaml overlaid with APP_* environment variables
func NewProvider() anvil.ConfigProvider[Config] {
	return conf.NewProvider[Config](configFS)
}
