package grpcadapter

import (
	"time"

	dw "github.com/specialistvlad/dagworker"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file holds the wire<->domain conversions for every message that
// mirrors a core type (see node.proto's per-message doc comments for which
// ones). Splitting it out keeps the RPC handlers themselves short enough to
// read as "validate, call the Manager, convert the result" — the conversion
// arithmetic is the part worth testing and reading in one place instead of
// interleaved four different ways.

// timeToProto omits the field entirely for a zero time rather than encoding
// the Unix epoch, so "unset" round-trips as unset instead of turning into a
// suspiciously specific 1970 timestamp on the wire.
func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func timeFromProto(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

func durationFromProto(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}

func durationToProto(d time.Duration) *durationpb.Duration {
	if d == 0 {
		return nil
	}
	return durationpb.New(d)
}

// statusToProto maps the closed, four-value dagworker.Status onto the wire
// enum. It is intentionally a total function with no default case reachable
// in practice: dagworker.Status never grows past StatusError (ADR-0001), so
// the exhaustive linter keeping this switch honest costs nothing.
func statusToProto(s dw.Status) pb.NodeStatus {
	switch s {
	case dw.StatusNew:
		return pb.NodeStatus_NODE_STATUS_NEW
	case dw.StatusInProgress:
		return pb.NodeStatus_NODE_STATUS_IN_PROGRESS
	case dw.StatusSuccess:
		return pb.NodeStatus_NODE_STATUS_SUCCESS
	case dw.StatusError:
		return pb.NodeStatus_NODE_STATUS_ERROR
	default:
		return pb.NodeStatus_NODE_STATUS_UNSPECIFIED
	}
}

func statusFromProto(s pb.NodeStatus) dw.Status {
	switch s {
	case pb.NodeStatus_NODE_STATUS_NEW, pb.NodeStatus_NODE_STATUS_UNSPECIFIED:
		return dw.StatusNew
	case pb.NodeStatus_NODE_STATUS_IN_PROGRESS:
		return dw.StatusInProgress
	case pb.NodeStatus_NODE_STATUS_SUCCESS:
		return dw.StatusSuccess
	case pb.NodeStatus_NODE_STATUS_ERROR:
		return dw.StatusError
	default:
		return dw.StatusNew
	}
}

func reasonToProto(r dw.Reason) pb.Reason {
	switch r {
	case dw.ReasonNone:
		return pb.Reason_REASON_NONE
	case dw.ReasonWorkerError:
		return pb.Reason_REASON_WORKER_ERROR
	case dw.ReasonTimeout:
		return pb.Reason_REASON_TIMEOUT
	case dw.ReasonUpstreamFailed:
		return pb.Reason_REASON_UPSTREAM_FAILED
	case dw.ReasonSkipped:
		return pb.Reason_REASON_SKIPPED
	case dw.ReasonCancelled:
		return pb.Reason_REASON_CANCELLED
	case dw.ReasonRemoved:
		return pb.Reason_REASON_REMOVED
	default:
		return pb.Reason_REASON_UNSPECIFIED
	}
}

func triggerToProto(t dw.TriggerRule) pb.TriggerRule {
	switch t {
	case dw.TriggerAllSuccess:
		return pb.TriggerRule_TRIGGER_RULE_ALL_SUCCESS
	case dw.TriggerAllDone:
		return pb.TriggerRule_TRIGGER_RULE_ALL_DONE
	case dw.TriggerNoneFailed:
		return pb.TriggerRule_TRIGGER_RULE_NONE_FAILED
	case dw.TriggerNoneFailedMinOneSuccess:
		return pb.TriggerRule_TRIGGER_RULE_NONE_FAILED_MIN_ONE_SUCCESS
	case dw.TriggerAlways:
		return pb.TriggerRule_TRIGGER_RULE_ALWAYS
	default:
		return pb.TriggerRule_TRIGGER_RULE_UNSPECIFIED
	}
}

// triggerFromProto treats UNSPECIFIED the same as ALL_SUCCESS: proto3's zero
// value must be a legal request, and ALL_SUCCESS is dagworker's own zero
// value and default trigger rule, so the two zeroes agree without a special
// case in every caller.
func triggerFromProto(t pb.TriggerRule) dw.TriggerRule {
	switch t {
	case pb.TriggerRule_TRIGGER_RULE_UNSPECIFIED, pb.TriggerRule_TRIGGER_RULE_ALL_SUCCESS:
		return dw.TriggerAllSuccess
	case pb.TriggerRule_TRIGGER_RULE_ALL_DONE:
		return dw.TriggerAllDone
	case pb.TriggerRule_TRIGGER_RULE_NONE_FAILED:
		return dw.TriggerNoneFailed
	case pb.TriggerRule_TRIGGER_RULE_NONE_FAILED_MIN_ONE_SUCCESS:
		return dw.TriggerNoneFailedMinOneSuccess
	case pb.TriggerRule_TRIGGER_RULE_ALWAYS:
		return dw.TriggerAlways
	default:
		return dw.TriggerAllSuccess
	}
}

func cascadeFromProto(c pb.CascadePolicy) dw.CascadePolicy {
	switch c {
	case pb.CascadePolicy_CASCADE_POLICY_UNSPECIFIED, pb.CascadePolicy_CASCADE_POLICY_REJECT:
		return dw.CascadeReject
	case pb.CascadePolicy_CASCADE_POLICY_DETACH:
		return dw.CascadeDetach
	case pb.CascadePolicy_CASCADE_POLICY_FAIL:
		return dw.CascadeFail
	default:
		return dw.CascadeReject
	}
}

func phaseToProto(p dw.Phase) pb.Phase {
	switch p {
	case dw.PhaseBlocked:
		return pb.Phase_PHASE_BLOCKED
	case dw.PhaseScheduled:
		return pb.Phase_PHASE_SCHEDULED
	case dw.PhaseReady:
		return pb.Phase_PHASE_READY
	case dw.PhaseClaimed:
		return pb.Phase_PHASE_CLAIMED
	case dw.PhaseDone:
		return pb.Phase_PHASE_DONE
	default:
		return pb.Phase_PHASE_UNSPECIFIED
	}
}

func retryPolicyToProto(r dw.RetryPolicy) *pb.RetryPolicy {
	return &pb.RetryPolicy{
		MaxAttempts: r.MaxAttempts,
		BaseDelay:   durationToProto(r.BaseDelay),
		MaxDelay:    durationToProto(r.MaxDelay),
	}
}

func retryPolicyFromProto(r *pb.RetryPolicy) dw.RetryPolicy {
	if r == nil {
		return dw.RetryPolicy{}
	}
	return dw.RetryPolicy{
		MaxAttempts: r.GetMaxAttempts(),
		BaseDelay:   durationFromProto(r.GetBaseDelay()),
		MaxDelay:    durationFromProto(r.GetMaxDelay()),
	}
}

func nodeToProto(n dw.Node) *pb.Node {
	return &pb.Node{
		Scope:     string(n.Scope),
		Id:        string(n.ID),
		Kind:      n.Kind,
		Status:    statusToProto(n.Status),
		Reason:    reasonToProto(n.Reason),
		Message:   n.Message,
		Attempt:   n.Attempt,
		Priority:  int32(n.Priority),
		Trigger:   triggerToProto(n.Trigger),
		Retry:     retryPolicyToProto(n.Retry),
		Payload:   n.Payload,
		Labels:    n.Labels,
		Seq:       uint64(n.Seq),
		CreatedAt: timeToProto(n.CreatedAt),
		UpdatedAt: timeToProto(n.UpdatedAt),
	}
}

// newNodeToSpec converts one AddNodes entry. The caller-supplied scope is not
// part of NewNode on the wire (it rides once on the request), so it is not
// part of dw.NodeSpec either — NodeSpec never carries a scope of its own.
func newNodeToSpec(n *pb.NewNode) dw.NodeSpec {
	deps := make([]dw.NodeID, 0, len(n.GetDeps()))
	for _, d := range n.GetDeps() {
		deps = append(deps, dw.NodeID(d))
	}
	return dw.NodeSpec{
		ID:       dw.NodeID(n.GetId()),
		Kind:     n.GetKind(),
		Payload:  n.GetPayload(),
		Labels:   n.GetLabels(),
		Priority: int16(n.GetPriority()),
		Trigger:  triggerFromProto(n.GetTrigger()),
		Retry:    retryPolicyFromProto(n.GetRetry()),
		Deps:     deps,
	}
}

func edgeFromProto(e *pb.Edge) dw.Edge {
	return dw.Edge{From: dw.NodeID(e.GetFromNodeId()), To: dw.NodeID(e.GetToNodeId())}
}

func scopeConfigToProto(c dw.ScopeConfig) *pb.ScopeConfig {
	return &pb.ScopeConfig{
		DefaultLeaseTimeout: durationToProto(c.DefaultLeaseTimeout),
		MinLeaseTimeout:     durationToProto(c.MinLeaseTimeout),
		MaxLeaseTimeout:     durationToProto(c.MaxLeaseTimeout),
		MaxAttempts:         c.MaxAttempts,
		RetryBaseDelay:      durationToProto(c.RetryBaseDelay),
		RetryMaxDelay:       durationToProto(c.RetryMaxDelay),
		TerminalRetention:   durationToProto(c.TerminalRetention),
		MaxSubscriberLag:    durationToProto(c.MaxSubscriberLag),
		MaxInFlight:         c.MaxInFlight,
		PayloadCap:          int32(c.PayloadCap),
		MaxBatchSize:        int32(c.MaxBatchSize),
		SweepBatchSize:      int32(c.SweepBatchSize),
		SweepInterval:       durationToProto(c.SweepInterval),
		PartitionCount:      c.PartitionCount,
	}
}

func scopeConfigFromProto(c *pb.ScopeConfig) dw.ScopeConfig {
	if c == nil {
		return dw.ScopeConfig{}
	}
	return dw.ScopeConfig{
		DefaultLeaseTimeout: durationFromProto(c.GetDefaultLeaseTimeout()),
		MinLeaseTimeout:     durationFromProto(c.GetMinLeaseTimeout()),
		MaxLeaseTimeout:     durationFromProto(c.GetMaxLeaseTimeout()),
		MaxAttempts:         c.GetMaxAttempts(),
		RetryBaseDelay:      durationFromProto(c.GetRetryBaseDelay()),
		RetryMaxDelay:       durationFromProto(c.GetRetryMaxDelay()),
		TerminalRetention:   durationFromProto(c.GetTerminalRetention()),
		MaxSubscriberLag:    durationFromProto(c.GetMaxSubscriberLag()),
		MaxInFlight:         c.GetMaxInFlight(),
		PayloadCap:          int(c.GetPayloadCap()),
		MaxBatchSize:        int(c.GetMaxBatchSize()),
		SweepBatchSize:      int(c.GetSweepBatchSize()),
		SweepInterval:       durationFromProto(c.GetSweepInterval()),
		PartitionCount:      c.GetPartitionCount(),
	}
}

func scopeStatsToProto(s dw.ScopeStats) *pb.ScopeStats {
	return &pb.ScopeStats{
		Total:      s.Total,
		Blocked:    s.Blocked,
		Scheduled:  s.Scheduled,
		Ready:      s.Ready,
		InProgress: s.InProgress,
		Succeeded:  s.Succeeded,
		Failed:     s.Failed,
		Sealed:     s.Sealed,
		Complete:   s.Complete,
		Cursor:     uint64(s.Cursor),
	}
}

func depCountsToProto(d dw.DepCounts) *pb.DepCounts {
	return &pb.DepCounts{
		Unsatisfied: d.Unsatisfied,
		Succeeded:   d.Succeeded,
		Skipped:     d.Skipped,
		Failed:      d.Failed,
	}
}

func inspectionToProto(insp dw.Inspection) *pb.Inspection {
	waiting := make([]string, 0, len(insp.Waiting))
	for _, id := range insp.Waiting {
		waiting = append(waiting, string(id))
	}
	successors := make([]string, 0, len(insp.Successors))
	for _, id := range insp.Successors {
		successors = append(successors, string(id))
	}
	return &pb.Inspection{
		Node:          nodeToProto(insp.Node),
		Phase:         phaseToProto(insp.Phase),
		Deps:          depCountsToProto(insp.Deps),
		Waiting:       waiting,
		Successors:    successors,
		Rank:          insp.Rank,
		LeaseDeadline: timeToProto(insp.LeaseDeadline),
		ReadyAt:       timeToProto(insp.ReadyAt),
	}
}

// leaseToProto builds the wire Lease, minting a fresh task token from exactly
// the fields the fencing check needs (see encodeTaskToken).
func leaseToProto(l dw.Lease) (*pb.Lease, error) {
	tok, err := encodeTaskToken(l)
	if err != nil {
		return nil, err
	}
	return &pb.Lease{
		TaskToken:       tok,
		FencingToken:    l.Epoch,
		Node:            nodeToProto(l.Node),
		LeaseExpiresAt:  timeToProto(l.Deadline),
	}, nil
}
