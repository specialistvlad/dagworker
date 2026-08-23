package grpcadapter

import (
	dw "github.com/specialistvlad/dagworker"
	pb "github.com/specialistvlad/dagworker/adapters/grpc/gen/dagworker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// encodeTaskToken packs the three fields a fenced lease actually needs —
// scope, node ID, epoch — into the opaque bytes a worker round-trips on
// ExtendLease/CompleteNode/FailNode/SkipNode. It is deliberately not the
// lease's Deadline or Node snapshot: those live in storage and would go stale
// the instant they were copied into a token a worker might hold for minutes.
func encodeTaskToken(l dw.Lease) ([]byte, error) {
	return proto.Marshal(&pb.TaskToken{
		Scope:  string(l.Scope),
		NodeId: string(l.NodeID),
		Epoch:  l.Epoch,
	})
}

// decodeTaskToken reverses encodeTaskToken, reconstructing exactly enough of
// a dagworker.Lease for [dagworker.Lease.Valid] to accept and for the store
// to fence on. A token that fails to parse is indistinguishable, from the
// caller's side, from one that was never issued — both are NOT_FOUND, per
// docs/research/13-grpc-worker-protocol.md §10's UNKNOWN_TASK_TOKEN case,
// which this adapter folds into the same ErrNotFound row as every other
// "no such lease" outcome.
func decodeTaskToken(b []byte) (dw.Lease, error) {
	var tok pb.TaskToken
	if err := proto.Unmarshal(b, &tok); err != nil || tok.GetScope() == "" || tok.GetNodeId() == "" || tok.GetEpoch() == 0 {
		return dw.Lease{}, status.Error(codes.NotFound, "unknown or malformed task token")
	}
	return dw.Lease{
		Scope:  dw.Scope(tok.GetScope()),
		NodeID: dw.NodeID(tok.GetNodeId()),
		Epoch:  tok.GetEpoch(),
	}, nil
}
