package grpcadapter

import (
	"context"

	dw "github.com/specialistvlad/dagworker"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
)

// controlServer implements pb.ControlServiceServer over a Manager. See
// workerServer's doc comment for why it is a separate type from workerServer
// rather than one type implementing both generated interfaces.
type controlServer struct {
	pb.UnimplementedControlServiceServer

	mgr      *dw.Manager
	cfg      serverConfig
	shutdown <-chan struct{}
}

// AddNodes creates nodes and their declared dependencies atomically.
func (c *controlServer) AddNodes(ctx context.Context, req *pb.AddNodesRequest) (*pb.AddNodesResponse, error) {
	scope := dw.Scope(req.GetScope())
	specs := make([]dw.NodeSpec, 0, len(req.GetNodes()))
	for _, n := range req.GetNodes() {
		specs = append(specs, newNodeToSpec(n))
	}
	if err := c.mgr.AddNodes(ctx, scope, specs); err != nil {
		return nil, mapError(err)
	}
	return &pb.AddNodesResponse{}, nil
}

// AddEdges records dependencies atomically.
func (c *controlServer) AddEdges(ctx context.Context, req *pb.AddEdgesRequest) (*pb.AddEdgesResponse, error) {
	edges := make([]dw.Edge, 0, len(req.GetEdges()))
	for _, e := range req.GetEdges() {
		edges = append(edges, edgeFromProto(e))
	}
	if err := c.mgr.AddEdges(ctx, dw.Scope(req.GetScope()), edges); err != nil {
		return nil, mapError(err)
	}
	return &pb.AddEdgesResponse{}, nil
}

// RemoveEdges drops dependencies atomically.
func (c *controlServer) RemoveEdges(ctx context.Context, req *pb.RemoveEdgesRequest) (*pb.RemoveEdgesResponse, error) {
	edges := make([]dw.Edge, 0, len(req.GetEdges()))
	for _, e := range req.GetEdges() {
		edges = append(edges, edgeFromProto(e))
	}
	if err := c.mgr.RemoveEdges(ctx, dw.Scope(req.GetScope()), edges); err != nil {
		return nil, mapError(err)
	}
	return &pb.RemoveEdgesResponse{}, nil
}

// RemoveNode deletes a node, applying the requested cascade policy to its
// successors.
func (c *controlServer) RemoveNode(ctx context.Context, req *pb.RemoveNodeRequest) (*pb.RemoveNodeResponse, error) {
	scope := dw.Scope(req.GetScope())
	id := dw.NodeID(req.GetNodeId())
	if err := c.mgr.RemoveNode(ctx, scope, id, cascadeFromProto(req.GetCascade())); err != nil {
		return nil, mapError(err)
	}
	return &pb.RemoveNodeResponse{}, nil
}

// GetNode returns a snapshot of one node.
func (c *controlServer) GetNode(ctx context.Context, req *pb.GetNodeRequest) (*pb.GetNodeResponse, error) {
	node, err := c.mgr.GetNode(ctx, dw.Scope(req.GetScope()), dw.NodeID(req.GetNodeId()))
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.GetNodeResponse{Node: nodeToProto(node)}, nil
}

// Inspect returns internal scheduling state for one node.
func (c *controlServer) Inspect(ctx context.Context, req *pb.InspectRequest) (*pb.InspectResponse, error) {
	insp, err := c.mgr.Inspect(ctx, dw.Scope(req.GetScope()), dw.NodeID(req.GetNodeId()))
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.InspectResponse{Inspection: inspectionToProto(insp)}, nil
}

// Cancel terminates the named nodes and everything downstream that can no
// longer run.
func (c *controlServer) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.CancelResponse, error) {
	ids := make([]dw.NodeID, 0, len(req.GetNodeIds()))
	for _, id := range req.GetNodeIds() {
		ids = append(ids, dw.NodeID(id))
	}
	if err := c.mgr.Cancel(ctx, dw.Scope(req.GetScope()), ids...); err != nil {
		return nil, mapError(err)
	}
	return &pb.CancelResponse{}, nil
}

// CancelScope terminates every unfinished node in the scope.
func (c *controlServer) CancelScope(ctx context.Context, req *pb.CancelScopeRequest) (*pb.CancelScopeResponse, error) {
	if err := c.mgr.CancelScope(ctx, dw.Scope(req.GetScope())); err != nil {
		return nil, mapError(err)
	}
	return &pb.CancelScopeResponse{}, nil
}

// Seal declares that no more nodes will be added to the scope.
func (c *controlServer) Seal(ctx context.Context, req *pb.SealRequest) (*pb.SealResponse, error) {
	if err := c.mgr.Seal(ctx, dw.Scope(req.GetScope())); err != nil {
		return nil, mapError(err)
	}
	return &pb.SealResponse{}, nil
}

// ScopeStats returns the scope's O(1) counters.
func (c *controlServer) ScopeStats(ctx context.Context, req *pb.ScopeStatsRequest) (*pb.ScopeStatsResponse, error) {
	stats, err := c.mgr.Stats(ctx, dw.Scope(req.GetScope()))
	if err != nil {
		return nil, mapError(err)
	}
	return &pb.ScopeStatsResponse{Stats: scopeStatsToProto(stats)}, nil
}

// Configure stores a scope's policy.
func (c *controlServer) Configure(ctx context.Context, req *pb.ConfigureRequest) (*pb.ConfigureResponse, error) {
	cfg := scopeConfigFromProto(req.GetConfig())
	if err := c.mgr.Configure(ctx, dw.Scope(req.GetScope()), cfg); err != nil {
		return nil, mapError(err)
	}
	return &pb.ConfigureResponse{}, nil
}

// ListNodes pages through a scope's nodes with a keyset cursor.
func (c *controlServer) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	statuses := make([]dw.Status, 0, len(req.GetStatuses()))
	for _, s := range req.GetStatuses() {
		statuses = append(statuses, statusFromProto(s))
	}
	res, err := c.mgr.ListNodes(ctx, dw.Scope(req.GetScope()), dw.ListOptions{
		Statuses: statuses,
		Kinds:    req.GetKinds(),
		Cursor:   req.GetCursor(),
		Limit:    int(req.GetLimit()),
	})
	if err != nil {
		return nil, mapError(err)
	}
	nodes := make([]*pb.Node, 0, len(res.Nodes))
	for _, n := range res.Nodes {
		nodes = append(nodes, nodeToProto(n))
	}
	return &pb.ListNodesResponse{Nodes: nodes, NextCursor: res.Next}, nil
}
