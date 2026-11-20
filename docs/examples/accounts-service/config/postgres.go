// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"time"

	"github.com/llingr/llingr-pgx/roles"
)

// Postgres path: `app.postgres.`
type Postgres struct {
	Connection Connection `yaml:"connection"`
	Roles      Roles      `yaml:"roles"`
}

type Connection struct {
	Host           string        `yaml:"host"`
	Port           uint16        `yaml:"port"`
	DatabaseName   string        `yaml:"databaseName"`
	SSLMode        string        `yaml:"sslMode"`
	ConnectTimeout time.Duration `yaml:"connectTimeout"`
	MaxIdleTime    time.Duration `yaml:"maxIdleTime"`
}

// Roles are the two the journal's migration files name: the owner migrates, the app serves
type Roles struct {
	Owner Role `yaml:"owner"`
	App   Role `yaml:"app"`
}

type Role struct {
	Username       roles.Username `yaml:"username"`
	Password       string         `yaml:"password"`
	MaxConnections int32          `yaml:"maxConnections"`
}
