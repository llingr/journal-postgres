// SPDX-FileCopyrightText: Copyright (c) 2026 The journal Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"testing"

	"github.com/llingr/journal/codec"
	"github.com/llingr/journal/codec/compress"
	"github.com/llingr/journal/codec/encrypt"
)

// The codec names live in two places, the codec package and the enum types in the schema, and
// nothing keeps them in agreement, so these tests parse the migration and compare.

const typesMigration = "0001_types.up.sql"

func TestCodecAlignmentDataFormat(t *testing.T) {
	assertNamesMatch(t, "events_data_format", namesOf(codec.AllFormats))
}

func TestCodecAlignmentDataCompression(t *testing.T) {
	assertNamesMatch(t, "events_data_compression", namesOf(compress.All))
}

func TestCodecAlignmentDataEncryption(t *testing.T) {
	assertNamesMatch(t, "events_data_encryption", namesOf(encrypt.All))
}

func namesOf[V ~string](values []V) []string {
	names := make([]string, 0, len(values))
	for _, value := range values {
		names = append(names, string(value))
	}
	return names
}

// assertNamesMatch fails unless the Go names and the enum's labels are the same set
func assertNamesMatch(t *testing.T, typeName string, goNames []string) {
	t.Helper()

	labels := readEnumLabels(t, typeName)
	slices.Sort(goNames)
	slices.Sort(labels)
	if !slices.Equal(goNames, labels) {
		t.Errorf("Go lists %v but %s %s lists %v", goNames, typesMigration, typeName, labels)
	}
}

// readEnumLabels extracts the quoted labels from "CREATE TYPE typeName AS ENUM (...)"
func readEnumLabels(t *testing.T, typeName string) []string {
	t.Helper()

	filePath := filepath.Join("schema", typesMigration)
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("reading %s: %v", filePath, err)
	}

	enumPattern := regexp.MustCompile(`CREATE TYPE\s+` + regexp.QuoteMeta(typeName) +
		`\s+AS ENUM\s*\(([^)]*)\)`)
	match := enumPattern.FindSubmatch(content)
	if match == nil {
		t.Fatalf("%s: no CREATE TYPE %s AS ENUM", typesMigration, typeName)
	}

	quoted := regexp.MustCompile(`'([^']*)'`).FindAllSubmatch(match[1], -1)
	if len(quoted) == 0 {
		t.Fatalf("%s: %s lists no labels", typesMigration, typeName)
	}
	labels := make([]string, 0, len(quoted))
	for _, label := range quoted {
		labels = append(labels, string(label[1]))
	}
	return labels
}
