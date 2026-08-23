# Security Policy

## Reporting a vulnerability

Please report security issues through GitHub's private vulnerability reporting:
**[Report a vulnerability](https://github.com/specialistvlad/dagworker/security/advisories/new)**.

Do not open a public issue. You should get an acknowledgement within a few days.

## Supported versions

The most recent minor release. This project is pre-1.0; there is no long-term
support branch yet.

## What is in scope

The library's own behaviour: a way to corrupt the graph, to bypass the fencing
check, to escape a scope boundary, to exceed a configured limit, or to cause
unbounded memory growth from bounded input.

## What is out of scope, by design

**Workers are trusted.** dagworker assumes workers are operated by the same
people as the Manager instances. The lease fencing token is a plain integer, so
a malicious worker can forge one and complete a node it does not hold, or replay
an old acknowledgement. It cannot corrupt the graph's structure, cross a scope
boundary, or exceed the payload cap.

This is a documented limitation rather than an oversight — see
[ADR-0035](docs/adr/0035-worker-trust-model-cooperative-workers-and-a-plain-integer-fencing-token.md),
which also records exactly what would change if untrusted workers became a
target: an HMAC over scope, node, epoch and deadline, plus key rotation. The
`ClaimToken` type is deliberately opaque so that change stays a backend concern
rather than a wire break.

If you are exposing the network adapters to workers you do not control, put an
authenticating proxy in front of them and treat the trust boundary as being
there, not here.

**Payloads are opaque bytes.** The library never interprets them. If your
workers deserialise a payload into something dangerous, that is your
deserialiser's threat model, not this library's.

**The daemon's admin listener** binds loopback by default and serves `pprof`
only behind an explicit flag. Exposing either is a deployment decision.
