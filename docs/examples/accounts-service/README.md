# accounts-service

A complete HTTP service on the journal library and this Postgres backend: each account is a
stream of events in Postgres, and each committed event is published, here to the log in place
of Kafka. It is built on anvil, anvil-koanf, anvil-zap, gin and llingr-pgx, and is its own
module.

## Run it

```sh
make run    # starts Postgres in Docker, migrates and serves on port 8090
make try    # in a second terminal: opens, upgrades, closes and reads back an account
make down   # stops Postgres and wipes its data
```

## API

| Route | Body | Answers |
|---|---|---|
| `POST /v1/accounts` | `{"tier": "STANDARD"}` | 201 and the account |
| `GET /v1/accounts/:account_id` | | 200 and the account |
| `POST /v1/accounts/:account_id/upgrades` | `{"tier": "PREMIUM"}` | 200 and the account |
| `POST /v1/accounts/:account_id/closure` | `{"reason": "moved bank"}` | 200 and the account |
| `GET /health` | | 200, or 503 once stopping |

Tiers are `STANDARD`, `PREMIUM` and `PLATINUM`. An unknown account is 404, and a change to a
closed account, or one that lost a race with another change, is 409.

## Where to look

- `cmd/main.go`: the wiring, with the journal's store between the database pool and the
  services.
- `cmd/database/schema_migrate.go`: the service applying the journal's migration files with
  llingr-pgx.
- `internal/model`: the account, its events, and the rules in `WithUpgraded` and `WithClosed`
  that refuse a change to a closed account.
- `internal/repos/accounts_repo`: the account kept as a journal stream.
- `internal/services/publishing`: the publisher a deployment would replace with a Kafka
  producer.

Settings are in `config/config.yaml`, and any can be overridden by an `APP_*` environment
variable, such as `APP_SERVER_PORT=9090`.
