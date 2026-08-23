// Command dagworkerd is dagworker's optional standalone daemon: a composition
// root that wires one configured [github.com/specialistvlad/dagworker.Store]
// backend to the gRPC and/or HTTP network adapters and serves them until a
// shutdown signal arrives.
//
// It is the one module in this repository allowed to import every other
// module — the core library, both adapters, and all three storage backends
// (docs/research/15-daemon-packaging-and-ops.md Part 2) — precisely so that
// nothing else has to. Everything adapter-specific stays in the adapters;
// everything daemon-specific (flag parsing, env lookup, signal handling,
// process-level logging setup) stays here.
//
// See README.md for every flag and environment variable, a Docker Compose
// example, and the shutdown-order rationale.
package main
