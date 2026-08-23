# Does this project make sense?

**Scope of this note:** not correctness, not code quality — those are covered elsewhere in
`docs/review/`. This is the step-back question: is dagworker solving a problem real people have,
is the niche it claims actually empty, is the scope right, and would a prospective adopter with a
real dependency graph and a real worker fleet come away from the README ready to use it.

**Research disclosure up front, honestly:** I attempted live web research on the alternatives
(River, Asynq, Sidekiq, Temporal, Airflow, Argo) via `WebSearch` as instructed, and every query
came back `"this session has used its web search budget (200 of 200)"` — the budget was already
exhausted before I made a single call, presumably by other work in this session pool. What follows
about those systems is general, well-established knowledge about widely-documented open-source
projects (release models, architecture, operational footprint), not freshly verified citations. I
am flagging that gap rather than papering over it, per this review's own standard for honesty.

---

## 1. The fact that reframes everything else

Before the design question, a factual one: how much real-world exposure does this design have?

```
$ git log --format='%ad' --date=short | sort -u
2026-08-22
2026-08-23
```

The entire repository — 15 research dossiers, 42 ADRs, the public API, three storage backends, a
conformance suite, two network adapters, a daemon, an end-to-end suite, and a performance suite
verified at one million nodes — was authored across **35 commits in under eight hours of
wall-clock commit time**, from `abe4910` (`chore: initialize repository with MIT license`,
2026-08-22 21:42) to `aa84ffe` (2026-08-23 05:37). There is one git tag, `v0.0.1`
(`CHANGELOG.md:75`), dated the same day the repository was created. There are no GitHub issues to
read, no production incident that shaped ADR-0041's amendments, no second team's workload that
stress-tested the trigger-rule set down to five. `go.mod:1-3` and the module version pin
`v0.0.1` in every downstream `go.mod` (`storage/postgres/go.mod:7`, `storage/redis/go.mod:7`,
`adapters/grpc/go.mod:5`) confirm this is pre-anything-released software.

This matters for a "does this make sense" review specifically because the design's central claims
— which trigger rules are the ones "that actually come up" (`README.md:105`), which failure modes
are worth a fencing protocol, which network protocol shape a worker fleet needs — are exactly the
claims that normally get validated by shipping something smaller and watching it get used. None of
that validation has happened. The 42 ADRs each list one decider: `Deciders: Vladyslav Kazantsev
(project owner)`, identically, on all 42 files. That is not a criticism of the individual
decisions (several are good engineering, discussed below) — it is evidence that the ADR apparatus,
the RFC-2119 normative contract, and the "backing research" citation on every decision are being
used to simulate a coordination problem (many stakeholders who need a durable record of why a
disputed call went the way it did) that does not exist yet, because there is exactly one
stakeholder. The mechanism a growing team adopts to manage real disagreement has been built first,
before there is anyone to disagree.

I do not think this makes the code wrong. I think it means every claim in the README about which
niche this fills, which use cases it is "the best tool" for, and what a fleet needs from it should
be read as a hypothesis, not a track record — and the rest of this review reads it that way.

---

## 2. What problem does it solve, and who has it?

The pitch, verbatim: *"You have work with dependencies between the items, workers that already
exist, and no appetite for a workflow engine with its own database, its own scheduler process, and
its own operational surface."* (`README.md:50-52`)

Stripped to its parts, the problem is: **a DAG-shaped scheduling primitive, embeddable in a
program that already has workers, without standing up a separate orchestrator service.** That is a
real technical gap in the sense that a plain task queue has no edges and a workflow engine is a
separate service. It is a real *problem people have* in a narrower sense than the README implies,
though: most teams that hit "I need dependency-aware task dispatch" solve it one of three ways
long before they'd go looking for a library like this —

1. **Hand-roll it on a jobs table.** A `status` column, a `depends_on` array or join table, `SELECT
   ... FOR UPDATE SKIP LOCKED`, and a hand-written "when a job finishes, check if its dependents are
   now unblocked" query. This is extremely common — it's what River (`storage/postgres`'s own
   design, per `docs/spec/01-contract.md §4.1`, uses the same primitive) and half of production
   Rails/Django/Go shops already have, often years before this library existed. dagworker's actual
   competitor for this population is not Temporal — it's their own migration file from three years
   ago, and dagworker doesn't make that comparison anywhere in the README.
2. **Chain jobs in a task queue's completion callback.** Sidekiq Pro's `Batch`, or "enqueue job B
   inside job A's success handler" in River/Asynq. This covers linear and simple fan-out/fan-in
   shapes — the overwhelming majority of real pipelines — without adopting a graph library at all.
   It breaks down exactly at: diamond dependencies with partial-failure semantics, `none_failed`
   /`all_done` trigger rules, and dynamic mid-run graph mutation. That breakdown is real, but it is
   a minority of the workloads that "have dependencies between items."
3. **Adopt a workflow engine anyway**, because the team is already running Kubernetes (Argo is
   free, in-cluster, and has a UI) or already has cross-service durable-execution needs beyond one
   DAG (Temporal), and the "operational surface" dagworker avoids was going to be paid for by
   something else in the stack regardless.

The users left in the gap between those three are: **a Go-centric platform or infra team, building
their *own* internal orchestration product or pipeline tool, who has concluded points 1-2 don't
scale to their actual graph shape and point 3's operational cost is not worth it for one
subsystem.** That is a real segment. It is also a narrow and technically sophisticated one — it is
not "people who need to run background jobs," it's "people building the thing that runs background
jobs for other people," and the README's opening framing (any reader with "work with dependencies")
casts a much wider net than the actual best-fit user.

---

## 3. Is the niche real, or is it the empty space between two things nobody needs there?

**Against task queues (River, Asynq, Sidekiq).** The README's own comparison
(`README.md:74-76`) is honest and correctly scoped: *"A queue has no edges... they are excellent
and simpler."* That's the right call and it's rare for a README to say "use the other thing" this
plainly — genuine credit here. But the comparison stops at "queues have no edges," and doesn't
address that River/Asynq/Sidekiq users solve the *common* dependency case (chains and simple
fan-out/fan-in) with the completion-callback pattern above, which needs zero new library and zero
new mental model. dagworker only wins once you need genuine multi-predecessor trigger semantics,
cycle rejection, or *runtime-discovered* graph shape — and the README never states that threshold
explicitly, so a reader can't self-select against it.

**Against workflow engines (Temporal, Airflow, Argo).** The README's dismissal (`README.md:72-73`)
— *"no DSL, no retries-with-compensation, no durable execution of your Go functions... use
Temporal"* — undersells the actual comparison in one specific way: **Temporal doesn't need a "DAG"
feature to get a dynamic dependency graph**, because a Temporal workflow is just code — loops,
conditionals, and `ExecuteActivity` calls express arbitrary dependency shapes without any DAG data
structure at all, dynamic or otherwise. So "the graph is dynamic" (`README.md:58-59`), presented as
a differentiator against workflow engines, isn't one against Temporal specifically; Temporal's
answer to "the shape is only known at runtime" is "write an if-statement." The real, defensible
differentiator against Temporal is purely operational: you don't run the Temporal server (or pay
for Temporal Cloud), you don't adopt its SDK's workflow-determinism constraints, and you keep your
existing worker code instead of rewriting it as Temporal activities. That's a legitimate reason to
pick dagworker over Temporal for one subsystem — but it is a much narrower claim than "the graph is
dynamic," and the README leads with the weaker version of its own best argument.

Against Airflow and Argo the comparison is more solid: both are batch/scheduled-DAG shaped (Airflow
increasingly supports runtime fan-out via dynamic task mapping, Argo via `withParam`, but neither
treats "add an edge to a live, already-executing DAG" as a first-class primitive the way
`AddEdge` after `Seal`-time does here), both require a service (scheduler + metadata DB + webserver
for Airflow; a Kubernetes cluster and controller for Argo) that a library-only user has deliberately
opted out of, and both come with a UI dagworker does not have (§5). The niche — "a live, mutable
dependency graph as an embedded primitive, not a scheduled batch DAG and not a separate cluster
resource" — is real against these two.

**Verdict on the niche:** it exists, but it is thinner than the README's four-way comparison
(queues / Temporal / Airflow / Argo, implicitly) suggests, because the strongest competing pattern
— a hand-rolled Postgres jobs table, which is what a large fraction of the target audience already
runs — is never named as an alternative at all, and it is the one dagworker most directly needs to
beat on "is a well-tested library actually worth the import over the 200 lines you already wrote."

---

## 4. Is the scope right?

**Right-sized: the core scheduling library and the three-backend conformance model.**
`manager.go`, `claim.go`, `store.go`, `subscribe.go`, and friends total 3,200 lines
(`wc -l manager.go claim.go store.go ...`) implementing one coherent idea — incremental
readiness, fenced leases, insert-time cycle rejection — behind one port, checked by one shared
suite (`dagstoretest/`, ~1,784 lines) that every backend must pass. This is a well-chosen boundary
and a genuinely good idea: a claim like "all three backends behave identically" is normally
marketing; here it's a `go test` invocation (ADR-0018, `docs/spec/01-contract.md:459-464`).

**Scope that got ahead of any validated need: the network surface.** `adapters/grpc` and
`adapters/http` together are 11,731 lines; `cmd/dagworkerd` is another 2,095 — roughly **43% of
the project's Go by line count** is a gRPC service, an HTTP/JSON service, and a daemon to host
them, for a project whose core pitch is "you already have workers... [it's] a library" (README
opening line, `README.md:3-4`). ADR-0037 justifies this scope addition by appeal to a specific
future user: *"a worker written in Python, Node, Rust, or Java can participate in the DAG without
embedding Go at all"* (`docs/adr/0037-network-surface-in-scope.md:14`), and further commits to
*"non-Go SDKs are produced from the same schema published to the Buf Schema Registry on every merge
to `main`"* (`docs/adr/0037-network-surface-in-scope.md:44`). Neither half of that promise exists:
there is no Python/Node/Rust/Java client anywhere in the tree (only a Go reference client under
`adapters/grpc/client` and `adapters/http/client`), and there is no `.github/workflows/*.yml` step
that pushes to Buf Schema Registry — `.github/workflows/` contains exactly `ci.yml` and
`codeql.yml`, neither of which mentions `buf push` or a schema registry
(`grep -rl "buf push\|Schema Registry" .github/workflows/*.yml` → no match). So the entire
justification for this 43%-of-the-codebase scope addition is a documented aspiration with zero
supporting evidence it will ever be exercised by anyone other than the Go reference client that
already ships. This is scope built for a hypothetical user, not a validated one — and it was built
*before* the core library it depends on has a single production user of its own.

**What should be cut, or at least deferred to a real v2:** `adapters/grpc`, `adapters/http`, and
`cmd/dagworkerd`. Not because the engineering is bad — it isn't, per the build passing cleanly and
the OpenAPI/`.proto` artifacts being genuinely committed rather than hand-waved — but because
shipping a cross-language wire protocol *before* the single-process, single-language use case has
any adopters inverts the normal order of validation, and doubles the v1.0 surface area a reviewer
or a future maintainer has to hold in their head for a benefit (non-Go workers) that isn't
implemented on the client side yet anyway.

**What's missing, and would block the use case the README claims:**

- **No observability surface beyond `Manager.Inspect`, and that is explicitly Go-API-only, debug-
  only, with no stability promise** (`docs/spec/01-contract.md:87-90`, "`Phase` MUST NOT appear on
  the event stream... reachable only via `Manager.Inspect`"). For the audience this project is
  actually aimed at — a platform team running someone else's pipeline through this graph — "which
  node is stuck and why" is an on-call question that gets asked at 3 a.m., and the honest answer
  today is "attach a debugger or build your own dashboard on `Subscribe`." Airflow's Grid view,
  Argo's UI, and Temporal's Web UI exist specifically because this question is common enough to be
  a product requirement, not a nice-to-have. Its total absence here — not even a read-only HTTP
  page shipped with `dagworkerd` — is the single biggest gap between what's built and what the
  claimed audience needs to actually operate this in production.
- **The claim-throughput ceiling of pull-based competition is known and undisclosed in the
  README.** The project's own research says so plainly: *"every claim serializes through one hot
  key/table region, so throughput has a ceiling that's a property of the storage engine, not the
  workload"* (`docs/research/07-work-distribution-across-instances.md:65`), and the spec confirms
  v1 ships the trivial `P=1` partition assigner, with real partitioning deferred to "the v0.5
  upgrade" (`docs/spec/01-contract.md:435`, "`PartitionCount` | 1 | v1 is pull-based; >1 is the
  v0.5 upgrade"). Redis compounds this: every mutating operation is one Lua script, and Lua
  scripts run on Redis's single command thread (`docs/research/05-redis-backend.md:973`, "share the
  same single-threaded command loop"). None of this appears in the README's "Multiple processes,
  one graph" pitch (`README.md:63-65`), which reads as unconditionally scalable ("no coordinator,
  no leader election, and no membership protocol") with no mention that the mechanism enabling that
  simplicity is also the mechanism capping throughput. The project is honest about this in its own
  research dossier and silent about it in the document a prospective adopter actually reads.
- **PostgreSQL bulk-insert cost makes the durable backend impractical for the workload shape the
  library exists to serve.** Seeding one million nodes takes **21 minutes** on Postgres versus 0.9s
  in-memory and 34s on Redis (`README.md:208`). The stated cause — six un-pipelined round trips per
  node, with `pgx.Batch` pipelining named as "the known fix" (`README.md:139, 213`) — is candid
  about the defect, but it is still a defect, unfixed, in the one backend that offers the durability
  guarantee ("full WAL durability," `docs/spec/01-contract.md:476`) a production deployment would
  pick Postgres *for*. A team choosing Postgres because they need the graph to survive a restart,
  and who need to seed a large batch — the exact "dynamic DAG with runtime-discovered fan-out"
  scenario this library's pitch centers on — hits a 21-minute cold start for a million nodes today.

---

## 5. Reading the README as a prospective user

Genuine strengths first, because they're real and worth naming precisely: the opening code sample
(`README.md:12-41`) is a complete, runnable program in 25 lines, which is the correct thing to lead
with. The "what it deliberately is not" section (`README.md:70-79`) is unusually disciplined for an
OSS README — most projects don't tell you to leave, and this one does, three times, specifically.
The performance section (`README.md:198-230`) states a methodology (ratio guards across a
thousandfold size range, not absolute thresholds a shared CI runner would break) before the
numbers, which is the right order and is more rigorous than the median library's benchmarks
section.

Where it would lose me as a prospective adopter with a real graph:

- **There is no path from the README to "how do I actually run this in production."** The example
  is one process, one goroutine loop, `memory.New()`. The moment I have "workers that already
  exist" — plural, possibly in another process, possibly not Go — the README's own stated primary
  use case, the document gives me a backend comparison table and a protocol comparison table, and
  then stops. No worked example of two processes sharing a Postgres-backed graph, no discussion of
  what a stuck lease looks like from the outside, no mention of what metrics exist to alert on
  (the README mentions exactly one exported metric, `topo_fastpath_hit_ratio`,
  `docs/spec/01-contract.md:291`, and never in the README itself).
- **The comparison section builds confidence, then the durability table quietly takes some of it
  back.** By the time a reader reaches "Backends" (`README.md:119-150`), the tone has established
  that this project measures its own claims. Then Redis is disclosed as losing "about a second of
  writes" on failover unless you opt into `WAIT`/`WAITAOF` per call (`README.md:141-143`) — correct
  and honest — but the *combination* of that with the Postgres seed-time number three sections later
  means neither of the two backends that offer "survives a restart" or "shared across processes"
  is a clean recommendation: one is slow to bulk-load, the other loses up to a second of writes on
  failover by default. A reader assembling "which backend do I actually run" from this README has
  to average two separate caveats from two separate tables to get an honest picture, and the
  Performance section's headline framing ("Nothing is O(n)... enforced by tests," `README.md:66-68`)
  doesn't prepare them for either one.
- **No section says what an incident looks like.** Given the library's own honesty that workers are
  "cooperative" and a malicious one "can forge a higher epoch and steal a node it does not hold, or
  replay an old ack" (`README.md` trust-model omission — this appears only in
  `docs/spec/01-contract.md:227-234`, not the README at all), a prospective adopter evaluating this
  for anything beyond a fully-trusted internal fleet needs that stated where they'll actually read
  it. Burying the trust model in the normative spec and leaving it out of the README is a real gap
  for anyone doing a five-minute evaluation, which is what a README is for.

Would it convert me? For the narrow reader described in §2 — a Go platform engineer who already
knows they need multi-predecessor trigger rules and already rejected Temporal on operational
grounds — yes, the pitch and the honesty land. For the broader reader the opening sentence invites
("work with dependencies between the items"), the README doesn't tell them which of those two
groups they're in, and the library is a poor fit for the second, larger group.

---

## 6. Concrete use cases — where would this genuinely be the best tool?

I looked for real, specific fits rather than generic ones. Two hold up; one I could not make work.

**1. Media/data pipelines with runtime-discovered fan-out, embedded in an existing Go
processing service.** Concretely: a video-ingestion service where a `probe` step determines the
segment count at runtime, each segment needs independent transcode/analyze steps, and a final
`mux` step depends on all of them (`TriggerAllSuccess`) while a `cleanup` step should run
regardless (`TriggerAllDone`) — exactly the shape the README's own failure-propagation example
models (`example_test.go:69-97`, `fetch → transform → load`, plus an `all_done` cleanup). The
company already runs a fleet of transcode workers (GPU-bound, long-running, prone to being
OOM-killed under memory pressure — fencing genuinely matters here, since a paused-not-dead worker
resuming and overwriting a supervisor's already-recorded failure would be a real, not theoretical,
bug), the graph shape is only known after the probe step runs (dynamic `AddNodes` mid-scope is the
actual differentiator, not "the graph is dynamic" in the abstract), and the team is Go-centric and
does not want to stand up Airflow or Temporal to schedule one subsystem. This is a genuinely strong
fit — modulo the observability gap in §4, which this exact use case will feel immediately, because
"why is segment 4,102 of 10,000 still transcoding" is the first question anyone asks in an
incident.

**2. An internal, non-Kubernetes CI/CD or release-orchestration tool with a self-hosted runner
fleet.** Build → test → [lint, security-scan] → publish → [notify, deploy-canary], with retries on
transient runner failures and a lease so a runner that vanishes mid-job doesn't strand the
pipeline in `InProgress` forever — this is the README's own worked example (`README.md:18-23`) and
it is a real, common shape. But this use case is **conditional, not unconditional**: if the org is
on Kubernetes, Argo Workflows already owns this exact niche, with years of hardening and a UI, for
free, in-cluster — and I would not recommend dagworker over Argo there. dagworker is the better
choice specifically for a team building a bespoke deploy tool that is *not* Kubernetes-native (a
single Go binary CD tool, workers as plain processes or VMs) and wants the dependency semantics
without adopting Argo's CRD model. Real, but narrower than "CI/CD" as a category.

**3. A distributed build/test-target scheduler for a monorepo (a from-scratch Bazel remote-execution
scheduler).** I could not make this one work. The dependency-graph-plus-workers shape matches on
paper, but Bazel (with BuildBarn/BuildBuddy-style remote execution) already solves this specific
problem with content-addressed caching, target hashing, and a CLI developers already use; adopting
dagworker here means also reimplementing artifact caching and incremental-rebuild semantics that
are the actual hard part of a build scheduler, while only getting the dependency-dispatch layer for
free. I list this as a **negative finding** because it's the most obvious-sounding fourth use case
and it doesn't survive contact with what the workload actually needs.

I could not find a third genuinely strong, unconditional use case beyond the two above. That itself
is a data point: the honest answer is "one strong fit, one conditional fit, one plausible-sounding
fit that falls apart on inspection" — not three unconditional wins.

---

## 7. Is the engineering proportionate to the value?

33,000 lines of Go and roughly 24,600 lines of Markdown (`find . -name "*.go" | xargs wc -l` →
32,747; `find docs -name "*.md" | xargs wc -l` → 24,565) is, by itself, not an unreasonable size for
a scheduling library with three conformance-tested backends and two adapters. The concern is not
the ratio of docs to code — it's the ratio of **certainty claimed to evidence available**. The ADR
corpus is 63,577 words and the research dossier corpus is 161,390 words
(`wc -w docs/adr/*.md`, `wc -w docs/research/*.md`) — together longer than a technical book — for
a library at `v0.0.1`, zero known deployments, and (per §1) a single decider on every one of the 42
decisions it took to get there. That volume of process is the kind an organization accumulates
*after* several teams have disagreed about a design choice and someone needed to write down why the
resolution went the way it did. Here it was produced in advance of any team using the thing at all,
which means every "backing research" citation is really "one person's synthesis of publicly
available material," dressed in the citation format of a decision that survived real internal
debate.

The code itself is a different, better story: `go build ./...` succeeds clean across the workspace
(core, both backends, both adapters, the daemon, `test/e2e`, `test/perf`), the module boundaries
described in ADR-0031 are real (`storage/postgres/go.mod`, `storage/redis/go.mod`,
`adapters/grpc/go.mod` each pull in exactly what they need and nothing else), and the core module's
zero-dependency claim holds (no entry in `go.list -m all` for the root module beyond the module
itself). That's proportionate, competent engineering. The disproportion is specifically in the
*documentation apparatus* claiming a level of settled design certainty — future SDKs, a v0.5
partition upgrade, a Buf Schema Registry pipeline, a five-rule trigger vocabulary declared "frozen"
before anyone outside this repository has used a fourth — that only real usage can actually earn.

---

## Verdict

The niche is real but thinner than the README implies: a live, mutable dependency graph as an
embedded library, for teams that have already rejected both a hand-rolled Postgres jobs table and a
separate workflow-engine service, is a genuine gap — but it's a narrow, technically sophisticated
audience, not the broad "work with dependencies" reader the opening line invites, and the README
never draws that line for them. The engineering that exists is honest and often better than most
OSS in the same category (measured performance claims, disciplined non-goals, a real
shared-conformance-suite that makes cross-backend parity a test rather than a promise), and I'd
trust the *design decisions* documented here more than most greenfield schedulers. But the project
has no track record whatsoever — built in under eight hours, tagged once, zero users, 42 unanimous
decisions from one person dressed as consensus — and roughly 43% of its code is a cross-language
network surface justified by a client story (non-Go SDKs, a schema registry pipeline) that does not
exist yet. I found one genuinely strong, specific use case (dynamic, runtime-discovered fan-out
pipelines embedded in an existing Go worker fleet, off Kubernetes), one conditional one (non-k8s
CI/CD orchestration), and one that sounds plausible but doesn't survive inspection (a Bazel-style
build scheduler). I would not stake a production system on this today — not because the mechanism
is wrong, but because nothing here has been staked on anything yet, and the confidence with which
it's written outpaces the evidence a reader can actually check.
