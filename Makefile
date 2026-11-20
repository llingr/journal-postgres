.PHONY: all build test coverage test-database test-database-stop lint

# bash for pipefail, so a test failure fails the run however its output is filtered
SHELL := /bin/bash

# The backend and the example service, each built and linted alone; only the backend has tests
MODULES := . docs/examples/accounts-service

# One throwaway Postgres serves every database suite
TEST_DATABASE_CONTAINER := journal-test-postgres
TEST_DATABASE_PORT := 55440
TEST_DATABASE_URL := postgres://postgres:pw@127.0.0.1:$(TEST_DATABASE_PORT)/postgres?sslmode=disable

# Built with a Go at least as new as go.mod's go directive, which it refuses otherwise
GOLANGCI_LINT_VERSION := v2.15.0

COVERAGE_PROFILE := .coverage/coverage.out

# The backend's packages, measured by every test that runs them: not the examples, nor the
# harness that exists to exercise the backend
COVERED_PACKAGES = $(shell go list ./... | grep -v -e /docs/ -e /tests/ | paste -sd, -)

all: build test

build:
	@for module in $(MODULES); do \
		(cd $$module && go build ./... && go vet ./...) || exit 1; done

# Every package under the race detector, one at a time as the suites share the database;
# CI=true turns a database test that cannot reach Postgres into a failure rather than a skip.
# go test's own coverage figure on each line is dropped, as it is that package's share of the
# whole backend; the report after the run gives each package's coverage by every test.
test: test-database
	@mkdir -p $(dir $(COVERAGE_PROFILE))
	@echo "go test -race -p 1 -count=1 -coverpkg=<the backend's packages> ./..."
	@set -o pipefail; CI=true TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -p 1 \
		-count=1 -coverpkg=$(COVERED_PACKAGES) -coverprofile=$(COVERAGE_PROFILE) ./... | \
		grep -vE 'no test files|^[[:space:]]+[^[:space:]]+[[:space:]]+coverage: ' | \
		sed -E 's/[[:space:]]+coverage: .*//'
	@$(MAKE) --no-print-directory coverage

# Each package's statement coverage from the last test run, then the total
coverage:
	@echo "coverage by package:"
	@awk -F'[: ]' 'NR > 1 { block = $$1 ":" $$2; statements[block] = $$3; \
		if ($$4 > 0) covered[block] = 1 } \
		END { for (block in statements) { split(block, parts, ":"); package = parts[1]; \
		sub("/[^/]*$$", "", package); total[package] += statements[block]; \
		if (block in covered) hit[package] += statements[block] } \
		for (package in total) printf "  %5.1f%%  %s\n", 100 * hit[package] / total[package], \
		package }' $(COVERAGE_PROFILE) | sort -k2
	@go tool cover -func=$(COVERAGE_PROFILE) | tail -1 | awk '{print "total:", $$NF}'

# Starts the test Postgres unless it is running, and waits until it takes connections
test-database:
	@docker inspect -f '{{.State.Running}}' $(TEST_DATABASE_CONTAINER) 2>/dev/null | grep -q true || \
		docker run -d --rm --name $(TEST_DATABASE_CONTAINER) -e POSTGRES_PASSWORD=pw \
			-p $(TEST_DATABASE_PORT):5432 postgres:18 >/dev/null
	@until docker exec $(TEST_DATABASE_CONTAINER) pg_isready -U postgres -h 127.0.0.1 -q; do \
		sleep 1; done

test-database-stop:
	docker rm -f $(TEST_DATABASE_CONTAINER)

lint:
	@for module in $(MODULES); do \
		docker run --rm -v "$(CURDIR)":/src -w /src/$$module \
			golangci/golangci-lint:$(GOLANGCI_LINT_VERSION) golangci-lint run ./... || exit 1; done
