.DEFAULT_GOAL := help

APP_URL := http://localhost:8090

.PHONY: help
help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

.PHONY: up
up: ## Start Postgres on port 5462
	docker compose up -d --wait postgres

.PHONY: down
down: ## Stop Postgres and wipe its volume
	docker compose down --volumes

.PHONY: run
run: up ## Run the service on port 8090; it migrates the journal's schema on start
	go run ./cmd

.PHONY: try
try: ## Open, upgrade, close and read back an account against the running service
	@account_id=$$(curl -fsS -X POST $(APP_URL)/v1/accounts -d '{"tier":"STANDARD"}' | \
		sed -E 's/.*"accountId":"([^"]+)".*/\1/'); \
	echo "opened $$account_id"; \
	curl -fsS -X POST $(APP_URL)/v1/accounts/$$account_id/upgrades -d '{"tier":"PREMIUM"}'; echo; \
	curl -fsS -X POST $(APP_URL)/v1/accounts/$$account_id/closure -d '{"reason":"moved bank"}'; echo; \
	curl -sS -X POST $(APP_URL)/v1/accounts/$$account_id/upgrades -d '{"tier":"PLATINUM"}'; echo; \
	curl -fsS $(APP_URL)/v1/accounts/$$account_id; echo

.PHONY: build
build: ## Build and vet
	go build ./...
	go vet ./...

.PHONY: psql
psql: ## Open psql on the accounts database as the owner
	docker compose exec postgres psql -U accounts_owner -d accounts
