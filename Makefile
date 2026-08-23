# dagworker
#
# This repository is a multi-module workspace: a root-level `go test ./...`
# silently skips nested modules, so every target loops over MODULES explicitly.
# Adding a module means adding it here and to go.work.

MODULES := . ./storage/postgres ./storage/redis \
           ./adapters/grpc ./adapters/http ./cmd/dagworkerd \
           ./test/perf ./test/e2e

# Two module sets, because there are two gates.
#
# FAST_MODULES is what `make check` compiles and runs. It leaves out test/perf,
# which holds measurements rather than assertions, and test/e2e, whose whole
# point is the full stack against real infrastructure. Neither belongs in a
# gate whose budget is ten seconds.
FAST_MODULES := $(filter-out ./test/perf ./test/e2e,$(MODULES))

# CORRECTNESS_MODULES is everything that asserts behaviour, which is everything
# except test/perf. `make integration` uses it. Running test/perf from the
# ordinary sweep meant the complexity guards executed TWICE per CI run -- 722 of
# `make integration`'s 826 seconds, and then again in `make complexity` -- for
# no additional information at all.
CORRECTNESS_MODULES := $(filter-out ./test/perf,$(MODULES))

# JOBS is how much to run at once. Compilation, not test execution, is where the
# time goes: on CI the whole suite runs in 10 seconds and the job takes 52. Note
# that parallelism only helps once the build cache is warm -- measured cold, a
# parallel sweep is SLOWER than a serial one, because `go build` already
# saturates every core on its own and eight of them just thrash.
JOBS ?= $(shell (nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null) || echo 4)

# GATE_JOBS is the per-part budget when the gate runs its parts concurrently.
GATE_JOBS ?= $(shell echo $$(( $(JOBS) / 4 + 1 )))

PAR := scripts/parallel.sh

# in-modules emits one "label<TAB>command" line per module, for PAR.
# The command must not contain a comma: $(call) splits on them.
define in-modules
$(foreach m,$(2),printf '%s\t%s\n' '$(m)' 'cd $(m) && $(1)';)
endef

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
	@# Serial, deliberately. golangci-lint takes a lock on its own analysis
	@# cache and refuses to run beside another copy of itself -- "Error:
	@# parallel golangci-lint is running" -- so a parallel sweep is a race that
	@# usually wins and sometimes reports a lint pass it never performed. It
	@# already saturates the machine across packages on its own; per-module
	@# concurrency measured a 20% gain and was not worth a gate that
	@# intermittently lies.
	@{ $(call in-modules,golangci-lint run --timeout=5m ./...,$(MODULES)) } | $(PAR) 1

.PHONY: test
test: ## Unit tests. No databases, no e2e, no measurements
	@{ $(call in-modules,go test -count=1 ./...,$(FAST_MODULES)) } | $(PAR) $(JOBS)

.PHONY: race
race: ## The same tests under the race detector, shuffled
	@{ $(call in-modules,go test -count=1 -race -shuffle=on ./...,$(FAST_MODULES)) } | $(PAR) $(JOBS)

.PHONY: up
up: ## Start PostgreSQL and Redis on their non-default test ports
	$(COMPOSE) up -d --wait

.PHONY: reset-db
reset-db: ## Empty the test databases without restarting them
	@# Tolerates a database no test has touched yet: the schema is created by
	@# the backend's own migrations on first use, so on a fresh container --
	@# every CI run -- there is nothing to truncate and TRUNCATE would fail
	@# the target before any test had a chance to run.
	@# lock_timeout, because TRUNCATE needs an AccessExclusiveLock and any
	@# connection left holding a row lock -- an interrupted test run, a killed
	@# benchmark -- deadlocks against it. Failing in five seconds with the
	@# server's own message beats hanging or reporting a deadlock nobody can
	@# place.
	@docker exec -i dagworker-test-postgres-1 psql -U dagworker -d dagworker -X -c "\
		SET lock_timeout = '5s'; \
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
integration: up reset-db ## Run every suite against real PostgreSQL and Redis
	@{ $(call in-modules,DAGWORKER_INTEGRATION=1 go test -count=1 -race -tags=integration -timeout 10m ./...,$(CORRECTNESS_MODULES)) } | $(PAR) $(JOBS)

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

.PHONY: throughput
throughput: up ## Absolute throughput numbers alone (part of `make benchmark`)
	@# -benchtime=1000x, a fixed iteration count rather than a wall-clock
	@# budget. Three of these benchmarks consume the graph, so the run length
	@# has to be something the graph can be sized against; with a time-based
	@# budget they exhaust a small graph instantly and skip, which looks like a
	@# pass and measures nothing.
	@#
	@# Absolute throughput on a shared machine is a number about that machine.
	@# The guard that protects complexity is the ratio sweep, and the headline
	@# figures come from `make million`.
	@# Two invocations, because one iteration count does not fit both shapes.
	@# The per-node benchmarks move one node per iteration; the *Batch ones move
	@# a hundred. At a shared 1000x that made AddNodesBatch insert 100,000 nodes
	@# into PostgreSQL -- 52 seconds of the budget in a single benchmark -- and
	@# made ClaimBatch demand 100,000 nodes from a 5,000-node graph, so it
	@# exhausted and skipped, which reads as a pass and measures nothing.
	cd test/perf && DAGWORKER_INTEGRATION=1 go test -count=1 -tags=integration \
		-run '^$$' -bench 'Claim$$|Claim/|GetNode|ClaimComplete' \
		-benchmem -benchtime=1000x -timeout 3m ./...
	cd test/perf && DAGWORKER_INTEGRATION=1 go test -count=1 -tags=integration \
		-run '^$$' -bench 'Batch' \
		-benchmem -benchtime=20x -timeout 3m ./...
	@echo
	@echo "For the full sweep including the million-node sizes:"
	@echo "  DAGWORKER_PERF_FULL=1 make benchmark   (slow, not bounded)"

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

# ------------------------------------------------------------------ the gates
#
# There are exactly two, and the split is by cost, not by kind.
#
#   make check      no databases, no containers, no measurements.   budget 10s
#   make benchmark  real databases, e2e, complexity, throughput.    budget 5m
#
# The budget is the design constraint, not an aspiration. A gate a developer
# will not wait for is a gate they route around, and one that takes fourteen
# minutes -- which this repository's did -- is not a gate at all, it is a thing
# that happens to you after you have already moved on.

.PHONY: check
check: ## Fast gate: tidy, lint, race, coverage. No databases. ~10s warm
	@printf '%s\t%s\n' \
	  tidy '$(MAKE) --no-print-directory tidy-check' \
	  lint '$(MAKE) --no-print-directory lint' \
	  race '$(MAKE) --no-print-directory race JOBS=$(GATE_JOBS)' \
	  cover '$(MAKE) --no-print-directory cover' \
	  | $(PAR) 4
	@echo
	@echo "check: green. 'make benchmark' runs everything this leaves out."

.PHONY: benchmark
benchmark: ## Everything check leaves out: real databases, e2e, complexity, throughput. ~5 min
	@$(MAKE) --no-print-directory integration
	@$(MAKE) --no-print-directory complexity
	@$(MAKE) --no-print-directory throughput
	@echo
	@echo "benchmark: green."

.PHONY: check-everything
check-everything: check benchmark million ## Both suites plus the 1M measurement (~30 min)

.PHONY: all
all: check benchmark ## Both suites
