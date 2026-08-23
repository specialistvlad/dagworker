package dagworker

import "fmt"

// Status is the entire public status vocabulary for a node. It has exactly four
// values and will not grow: everything a fifth value might express is carried
// by [Reason] instead. See docs/adr/0001-public-status-vocabulary-is-exactly-four-values.md.
type Status uint8

const (
	// StatusNew means the node exists and has not yet completed an attempt
	// successfully. It covers both "waiting on predecessors" and "claimable
	// right now"; that distinction is scheduling detail, reachable via
	// [Manager.Inspect], and is deliberately not a public status because
	// adding an edge can flip it with no actor involved.
	StatusNew Status = iota

	// StatusInProgress means a worker holds a valid lease on the node.
	StatusInProgress

	// StatusSuccess is terminal: a worker acked success.
	StatusSuccess

	// StatusError is terminal: the node did not succeed. [Node.Reason] says why.
	StatusError
)

// Terminal reports whether s is a final status. A terminal node changes only by
// being deleted under a retention policy.
func (s Status) Terminal() bool { return s == StatusSuccess || s == StatusError }

// String implements [fmt.Stringer].
func (s Status) String() string {
	switch s {
	case StatusNew:
		return "new"
	case StatusInProgress:
		return "in_progress"
	case StatusSuccess:
		return "success"
	case StatusError:
		return "error"
	default:
		return fmt.Sprintf("status(%d)", uint8(s))
	}
}

// MarshalText implements [encoding.TextMarshaler] so Status round-trips through
// JSON, YAML, and log/slog as a stable name rather than an integer whose meaning
// could drift.
func (s Status) MarshalText() ([]byte, error) {
	if s > StatusError {
		return nil, fmt.Errorf("%w: status %d", ErrInvalidArgument, uint8(s))
	}
	return []byte(s.String()), nil
}

// UnmarshalText implements [encoding.TextUnmarshaler].
func (s *Status) UnmarshalText(text []byte) error {
	switch string(text) {
	case "new":
		*s = StatusNew
	case "in_progress":
		*s = StatusInProgress
	case "success":
		*s = StatusSuccess
	case "error":
		*s = StatusError
	default:
		return fmt.Errorf("%w: unknown status %q", ErrInvalidArgument, text)
	}
	return nil
}

// Reason explains a node's most recent significant outcome. It is a closed set;
// free text belongs in [Node.Message].
//
// How Reason relates to [Status] is normative:
//
//	Status New,        Attempt 0  -> ReasonNone
//	Status New,        Attempt >0 -> why the most recent attempt failed (awaiting retry)
//	Status InProgress             -> why the previous attempt failed, or ReasonNone
//	Status Success                -> ReasonNone
//	Status Error                  -> why the node is terminally failed
type Reason uint8

const (
	// ReasonNone means no significant outcome has been recorded.
	ReasonNone Reason = iota

	// ReasonWorkerError means a worker reported failure via [Manager.Nack].
	ReasonWorkerError

	// ReasonTimeout means the lease deadline elapsed with no acknowledgement.
	// It is an error reason, not a status: no production workflow engine
	// surveyed makes timeout a peer of success and failure.
	ReasonTimeout

	// ReasonUpstreamFailed means a predecessor failed and this node's trigger
	// rule can no longer be satisfied.
	ReasonUpstreamFailed

	// ReasonSkipped means the trigger rule is provably unsatisfiable for a
	// reason that is not an upstream failure.
	ReasonSkipped

	// ReasonCancelled means [Manager.Cancel] or [Manager.CancelScope] was called.
	ReasonCancelled

	// ReasonRemoved means a predecessor was removed under [CascadeFail].
	ReasonRemoved
)

// String implements [fmt.Stringer].
func (r Reason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonWorkerError:
		return "worker_error"
	case ReasonTimeout:
		return "timeout"
	case ReasonUpstreamFailed:
		return "upstream_failed"
	case ReasonSkipped:
		return "skipped"
	case ReasonCancelled:
		return "cancelled"
	case ReasonRemoved:
		return "removed"
	default:
		return fmt.Sprintf("reason(%d)", uint8(r))
	}
}

// MarshalText implements [encoding.TextMarshaler].
func (r Reason) MarshalText() ([]byte, error) {
	if r > ReasonRemoved {
		return nil, fmt.Errorf("%w: reason %d", ErrInvalidArgument, uint8(r))
	}
	return []byte(r.String()), nil
}

// UnmarshalText implements [encoding.TextUnmarshaler].
func (r *Reason) UnmarshalText(text []byte) error {
	switch string(text) {
	case "none":
		*r = ReasonNone
	case "worker_error":
		*r = ReasonWorkerError
	case "timeout":
		*r = ReasonTimeout
	case "upstream_failed":
		*r = ReasonUpstreamFailed
	case "skipped":
		*r = ReasonSkipped
	case "cancelled":
		*r = ReasonCancelled
	case "removed":
		*r = ReasonRemoved
	default:
		return fmt.Errorf("%w: unknown reason %q", ErrInvalidArgument, text)
	}
	return nil
}

// Phase is internal scheduling detail, exposed read-only through
// [Manager.Inspect] for debugging and admin tooling. It carries no stability
// promise across minor versions, never appears on the event stream, and never
// appears in a wire format.
type Phase uint8

const (
	// PhaseBlocked means at least one predecessor is unsatisfied.
	PhaseBlocked Phase = iota

	// PhaseScheduled means dependencies are satisfied but a retry backoff has
	// not yet elapsed.
	PhaseScheduled

	// PhaseReady means the node is claimable now.
	PhaseReady

	// PhaseClaimed means a worker holds a valid lease.
	PhaseClaimed

	// PhaseDone means the node is terminal. Which terminal outcome, and why,
	// is [Status] and [Reason] — Phase deliberately does not duplicate them.
	PhaseDone
)

// Status returns the public status that p maps onto. The mapping is total and
// fixed: it is the only place the internal and public vocabularies meet.
func (p Phase) Status() Status {
	switch p {
	case PhaseBlocked, PhaseScheduled, PhaseReady:
		return StatusNew
	case PhaseClaimed:
		return StatusInProgress
	case PhaseDone:
		fallthrough
	default:
		// PhaseDone alone is ambiguous between Success and Error; callers read
		// Node.Status directly. Reported as StatusError so a bug cannot make a
		// failed node look successful.
		return StatusError
	}
}

// String implements [fmt.Stringer].
func (p Phase) String() string {
	switch p {
	case PhaseBlocked:
		return "blocked"
	case PhaseScheduled:
		return "scheduled"
	case PhaseReady:
		return "ready"
	case PhaseClaimed:
		return "claimed"
	case PhaseDone:
		return "done"
	default:
		return fmt.Sprintf("phase(%d)", uint8(p))
	}
}
