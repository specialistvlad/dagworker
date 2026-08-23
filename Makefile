# dagworker
#
# This repository is a multi-module workspace: a root-level `go test ./...`
# silently skips nested modules, so every target loops over MODULES explicitly.
# Adding a module means adding it here and to go.work.

MODULES := . ./storage/postgres ./storage/redis \
           ./adapters/grpc ./adapters/http ./cmd/dagworkerd \
           ./test/perf ./test/e2e

COMPOSE := docker compose -f test/e2e/docker-compose.test.yml
COVERPKG := github.com/specialistvlad/dagworker,github.com/specialistvlad/dagworker/internal/pq,github.com/specialistvlad/dagworker/storage/memory
COVER_MIN := 95

export DAGWORKER_POSTGRES_DSN ?= postgres://dagworker:dagworker@127.0.0.1:15432/dagworker?sslmode=disable
export DAGWORKER_REDIS_ADDR   ?= 127.0.0.1:16379

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: fmt
fmt: ## Format every module
	golangci-lint fmt ./...

.PHONY: lint
lint: ## Run the strict linter over every module
	@for m in $(MODULES); do \
		echo "==> lint $$m"; \
		(cd $$m && golangci-lint run --timeout=5m ./...) || exit 1; \
	done

.PHONY: test
test: ## Unit and feature tests (no databases required)
	@for m in $(MODULES); do \
		echo "==> test $$m"; \
		(cd $$m && go test -count=1 ./...) || exit 1; \
	done

.PHONY: race
race: ## The same tests under the race detector, shuffled
	@for m in $(MODULES); do \
		echo "==> race $$m"; \
		(cd $$m && go test -count=1 -race -shuffle=on ./...) || exit 1; \
	done

.PHONY: up
up: ## Start PostgreSQL and Redis on their non-default test ports
	$(COMPOSE) up -d --wait

.PHONY: reset-db
reset-db: ## Empty the test databases without restarting them
	@# Tolerates a database no test has touched yet: the schema is created by
	@# the backend's own migrations on first use, so on a fresh container --
	@# every CI run -- there is nothing to truncate and TRUNCATE would fail
	@# the target before any test had a chance to run.
	@docker exec -i dagworker-test-postgres-1 psql -U dagworker -d dagworker -X -c "\
		DO \$$\$$ BEGIN \
		  IF EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'dagw') THEN \
		    TRUNCATE dagw.events, dagw.edges, dagw.nodes, dagw.scopes CASCADE; \
		  END IF; \
		END \$$\$$;" >/dev/null
	@docker exec -i dagworker-test-redis-1 redis-cli FLUSHALL >/dev/null
	@echo "test databases emptied"

.PHONY: down
down: ## Stop them and discard their data
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail the test databases' logs
	$(COMPOSE) logs -f

.PHONY: integration
integration: up ## Run every suite against real PostgreSQL and Redis
	@for m in $(MODULES); do \
		echo "==> integration $$m"; \
		(cd $$m && DAGWORKER_INTEGRATION=1 go test -count=1 -race -tags=integration -timeout 25m ./...) || exit 1; \
	done

.PHONY: cover
cover: ## Measure coverage of the shipped library and enforce the floor
	go test -count=1 -covermode=atomic -coverprofile=coverage.out -coverpkg=$(COVERPKG) ./...
	@go tool cover -func=coverage.out | tail -1
	@go tool cover -func=coverage.out | tail -1 | awk '{ \
		pct = $$3 + 0; \
		if (pct < $(COVER_MIN)) { \
			printf("\ncoverage is %.1f%%, below the %d%% floor\n", pct, $(COVER_MIN)); exit 1; \
		} \
		printf("coverage %.1f%% meets the %d%% floor\n", pct, $(COVER_MIN)); \
	}'

.PHONY: cover-html
cover-html: cover ## Open the coverage report in a browser
	go tool cover -html=coverage.out

.PHONY: complexity
complexity: up reset-db ## Prove no operation's cost grows with the size of the graph
	cd test/perf && DAGWORKER_INTEGRATION=1 go test -count=1 -tags=integration \
		-timeout 30m -run TestComplexity -v ./...

.PHONY: complexity-quick
complexity-quick: ## The same guards, in-memory only, no databases needed
	cd test/perf && go test -count=1 -timeout 10m -run TestComplexity -v ./...

.PHONY: million
million: up reset-db ## Measure every backend at 1,000,000 nodes (slow: ~25 min)
	@echo "Seeding a million nodes per backend. PostgreSQL takes about 20 minutes."
	@# The output is captured rather than piped: piping into grep would make the
	@# pipeline exit with grep's status, so a failed measurement would report
	@# success. The log is kept on failure so there is something to read.
	@log=$$(mktemp -t dagworker-million); \
	  (cd test/perf && DAGWORKER_INTEGRATION=1 DAGWORKER_MILLION=1 go test -count=1 -tags=integration \
	      -timeout 60m -run TestMillionNodes -v ./...) > $$log 2>&1; \
	  status=$$?; \
	  grep -E 'seeded|at 1000000|--- (PASS|FAIL)' $$log || true; \
	  $(MAKE) --no-print-directory reset-db; \
	  if [ $$status -ne 0 ]; then echo "FAILED - full output: $$log"; exit $$status; fi; \
	  rm -f $$log

.PHONY: bench
bench: ## Throughput benchmarks (absolute numbers; compare with benchstat)
	cd test/perf && go test -count=1 -run '^$$' -bench . -benchmem -timeout 30m ./...
	@echo
	@echo "note: the benchmarks leave their graphs behind. A million nodes in a"
	@echo "shared database slows every later test enough to make the"
	@echo "timing-sensitive ones flaky. Run 'make reset-db' before 'make integration'."

.PHONY: tidy
tidy: ## Tidy every module's dependencies
	@for m in $(MODULES); do \
		echo "==> tidy $$m"; \
		(cd $$m && go mod tidy) || exit 1; \
	done

.PHONY: tidy-check
tidy-check: ## Fail if any go.mod or go.sum is not tidy
	@for m in $(MODULES); do \
		(cd $$m && go mod tidy -diff) || { echo "$$m: go.mod/go.sum is not tidy"; exit 1; }; \
	done

.PHONY: vuln
vuln: ## Check dependencies against the Go vulnerability database
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

.PHONY: check
check: tidy-check lint race cover ## Everything CI runs that needs no database

.PHONY: check-all
check-all: check integration complexity ## Everything except the million-node run

.PHONY: check-everything
check-everything: check-all million ## check-all plus the 1M measurement (~30 min)
