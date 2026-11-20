// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package config

import "time"

// Server path: `app.server.`
type Server struct {
	Port              int           `yaml:"port"`
	ReadHeaderTimeout time.Duration `yaml:"readHeaderTimeout"`
}
