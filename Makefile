# dagworker
#
# This repository is a multi-module workspace: a root-level `go test ./...`
# silently skips nested modules, so every target loops over MODULES explicitly.
# Adding a module means adding it here and to go.work.

MODULES := . ./storage/postgres ./storage/redis ./test/perf

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
		(cd $$m && DAGWORKER_INTEGRATION=1 go test -count=1 -race -tags=integration ./...) || exit 1; \
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
complexity: ## Prove no operation's cost grows with the size of the graph
	cd test/perf && go test -count=1 -timeout 30m -run TestComplexity -v ./...

.PHONY: bench
bench: ## Throughput benchmarks (absolute numbers; compare with benchstat)
	cd test/perf && go test -count=1 -run '^$$' -bench . -benchmem -timeout 30m ./...

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
check-all: check integration complexity ## Everything, databases included
