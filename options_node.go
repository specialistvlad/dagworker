package dagworker

import "time"

// NodeOption configures a node at creation. The interface is opaque so options
// can gain behaviour later without the set of legal values becoming part of the
// API's shape.
type NodeOption interface{ applyNode(*NodeSpec) }

type nodeOptionFunc func(*NodeSpec)

func (f nodeOptionFunc) applyNode(s *NodeSpec) { f(s) }

// WithDeps declares predecessors. They must already exist in the same scope, or
// appear earlier in the same [Manager.AddNodes] batch.
func WithDeps(deps ...NodeID) NodeOption {
	return nodeOptionFunc(func(s *NodeSpec) { s.Deps = append(s.Deps, deps...) })
}

// WithKind sets the ready-set partition. A worker can claim by kind, which is
// how one graph feeds several worker pools without either scanning or a
// secondary index.
//
// Kind is a partition key, so its cardinality should stay small — a handful of
// pools, not one value per node. Per-node metadata belongs in [WithLabels].
func WithKind(kind string) NodeOption {
	return nodeOptionFunc(func(s *NodeSpec) { s.Kind = kind })
}

// WithPriority orders the ready set: higher is claimed first, and equal
// priorities are served in insertion order.
func WithPriority(p int16) NodeOption {
	return nodeOptionFunc(func(s *NodeSpec) { s.Priority = p })
}

// WithTrigger sets the rule deciding when the node becomes claimable given its
// predecessors' outcomes. The default, [TriggerAllSuccess], is what most graphs
// want.
func WithTrigger(t TriggerRule) NodeOption {
	return nodeOptionFunc(func(s *NodeSpec) { s.Trigger = t })
}

// WithLabels attaches metadata for subscription filtering. Labels are not an
// indexed claim predicate; use [WithKind] for that.
func WithLabels(labels map[string]string) NodeOption {
	return nodeOptionFunc(func(s *NodeSpec) {
		if s.Labels == nil {
			s.Labels = make(map[string]string, len(labels))
		}
		for k, v := range labels {
			s.Labels[k] = v
		}
	})
}

// WithRetry overrides the scope's retry policy for this node, field by field:
// a zero field inherits rather than disabling.
func WithRetry(maxAttempts uint32, base, maxDelay time.Duration) NodeOption {
	return nodeOptionFunc(func(s *NodeSpec) {
		s.Retry = RetryPolicy{MaxAttempts: maxAttempts, BaseDelay: base, MaxDelay: maxDelay}
	})
}

// WithMaxAttempts caps how many times the node may be attempted, including the
// first. One means no retry.
func WithMaxAttempts(n uint32) NodeOption {
	return nodeOptionFunc(func(s *NodeSpec) { s.Retry.MaxAttempts = n })
}
