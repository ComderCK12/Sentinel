# Development guide

How to build, test, lint, and run Sentinel's services locally from the
terminal. This is the real, working version of what the README's
"Getting started" pointed at — the Makefile it used to reference doesn't
exist in this repo, so every command below is one that actually runs.

## Prerequisites

| Tool | Version | Used for |
| --- | --- | --- |
| [Go](https://go.dev/dl/) | 1.26+ (`go.work` pins `1.26.6`) | building/testing/linting every Go service |
| [Docker](https://www.docker.com/) + Docker Compose | any recent version | local Redpanda (Kafka), Postgres, Redis |
| [golangci-lint](https://golangci-lint.run/) | v2 (matches `.golangci.yml`'s `version: "2"` schema) | linting — `brew install golangci-lint` or see CI's `goinstall` approach below |
| Python 3.12 + pip | 3.12 | `model-serving` (currently a skeleton; real code lands in Phase 3) |
| [ruff](https://docs.astral.sh/ruff/) | latest | Python linting |
| `psql`, `redis-cli` | any | optional — manually inspecting Postgres/Redis state while testing |

Go module layout: this is a multi-module workspace (`go.work`), not a
single module — `ingestion`, `decision-service`, `stream-processor`,
`shared`, and `loadgen` each have their own `go.mod`. **`go build ./...`
from the repo root does not work** (`go.work`'s root isn't itself a
module) — every command below targets each module explicitly, the same
way `.github/workflows/ci.yml` does.

## 1. Bring up local infrastructure

```bash
docker compose up -d
docker compose ps   # wait until every service shows "healthy"
```

This starts Redpanda (Kafka) on `localhost:19092`, Postgres on
`localhost:5435`, and Redis on `localhost:6379`.

> **Known gotcha:** if you have a Homebrew `redis-server` already running,
> it also binds `6379` and `localhost:6379` resolves to *that* process, not
> the Docker container — `docker exec sentinel-redis redis-cli ...`
> inspects the wrong instance. Use a plain `redis-cli -p 6379 ...` from
> the host to see what the services actually talk to. See
> `docs/testing.md` for how this was found.

## 2. Compile

```bash
go build ./ingestion/... ./decision-service/... ./stream-processor/... ./shared/... ./loadgen/...
```

Or one module at a time, e.g. `go build ./stream-processor/...`.

## 3. Test

```bash
go test ./ingestion/... ./decision-service/... ./stream-processor/... ./shared/... ./loadgen/...
```

Unit test coverage today is concentrated in `stream-processor`'s
storage-agnostic packages — `internal/features` (6 tests) and
`internal/replay` (4 tests) — since those were deliberately built to not
depend on Redis/Postgres/Kafka. `ingestion` and `decision-service` have no
unit tests yet; their behavior so far has only been verified through live
runs against the real local stack, logged in `docs/testing.md`.

**Known gap:** `go test` isn't part of `.github/workflows/ci.yml` yet —
CI currently only builds, vets, and lints. Worth adding once there's
enough test coverage across modules to make it meaningful.

**Convention going forward:** when you add or change functionality, add
or update the corresponding `_test.go` file in the same change — don't
let test files drift behind the code they're supposed to cover. For
anything that can't reasonably be unit tested (Redis/Kafka/Postgres
integration behavior), add a new dated section to `docs/testing.md`
instead, following its existing format (steps, expected result, what was
actually run) — that's what's documented every Phase 1-2 task so far.

## 4. Vet

```bash
go vet ./ingestion/... ./decision-service/... ./stream-processor/... ./shared/... ./loadgen/...
```

## 5. Format check

```bash
gofmt -l ingestion/ decision-service/ stream-processor/ shared/ loadgen/
```

Empty output = clean. `gofmt -w <path>` fixes anything it lists.

## 6. Lint

```bash
cd stream-processor && golangci-lint run ./...   # repeat per module: ingestion, decision-service, shared, loadgen
```

Matches CI's `--timeout=5m` run across the same explicit module list.
golangci-lint must be v2 — the config at `.golangci.yml` uses the v2
schema (`version: "2"`, `formatters` split out from `linters`); a v1
binary rejects it outright. If `golangci-lint version` doesn't say `2.x`,
reinstall (`brew install golangci-lint` picks up v2; see the `ci.yml`
comments for why `golangci-lint-action`'s `goinstall` mode was needed in
CI specifically).

### Python lint (model-serving)

```bash
cd model-serving
pip install -r requirements.txt ruff
ruff check .
```

## 7. Run a service

Every service reads its config from environment variables with sane
local defaults — none of these require flags. Build first (step 2), then:

```bash
# Ingestion — HTTP endpoint + Kafka producer
KAFKA_BROKERS=localhost:19092 REDIS_ADDR=localhost:6379 PORT=8080 \
  go run ./ingestion/cmd/server

# Decision service — Kafka consumer, dummy rule, Postgres
KAFKA_BROKERS=localhost:19092 \
  POSTGRES_DSN="postgres://sentinel:sentinel@localhost:5435/sentinel" \
  PORT=8083 \
  go run ./decision-service/cmd/server

# Stream processor — Kafka consumer, feature computation, Redis
# Note: defaultPort is 8084, not 8081 — 8081 collides with Redpanda's
# schema registry, which docker-compose.yml also maps to the host.
KAFKA_BROKERS=localhost:19092 REDIS_ADDR=localhost:6379 PORT=8084 \
  go run ./stream-processor/cmd/worker

# Offline feature replay — one-shot batch job, not a long-running server
POSTGRES_DSN="postgres://sentinel:sentinel@localhost:5435/sentinel" \
  OUTPUT_PATH=features_dataset.csv \
  go run ./stream-processor/cmd/offline-replay
```

Health checks once a service is up:

```bash
curl -s localhost:8080/health  # ingestion
curl -s localhost:8083/health  # decision-service
curl -s localhost:8084/health  # stream-processor
```

### Generating load

```bash
go run ./loadgen -n 1000 -dup-rate 0.1 -concurrency 50 -url http://localhost:8080/v1/events
```

`-n` total requests, `-dup-rate` fraction that deliberately re-send an
already-used `event_id` (exercises idempotency), `-concurrency` worker
count, `-url` the ingestion endpoint to hit.

## 8. Tear down

```bash
docker compose down        # stop and remove containers, keep volumes (data persists)
docker compose down -v     # also wipe Postgres/Redis/Kafka data
```
