package grpcadapter

import (
	"testing"

	dw "github.com/specialistvlad/dagworker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestTaskTokenRoundTrip confirms the token carries exactly what a fenced
// operation needs and nothing that could go stale — see encodeTaskToken's
// doc comment on why Deadline and Node are deliberately excluded.
func TestTaskTokenRoundTrip(t *testing.T) {
	t.Parallel()

	want := dw.Lease{Scope: "s1", NodeID: "n1", Epoch: 7}
	tok, err := encodeTaskToken(want)
	if err != nil {
		t.Fatalf("encodeTaskToken: %v", err)
	}

	got, err := decodeTaskToken(tok)
	if err != nil {
		t.Fatalf("decodeTaskToken: %v", err)
	}
	if got.Scope != want.Scope || got.NodeID != want.NodeID || got.Epoch != want.Epoch {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
	if !got.Valid() {
		t.Fatalf("decoded lease %+v is not Valid()", got)
	}
}

// TestDecodeTaskTokenMalformed pins docs/research/13-grpc-worker-protocol.md
// §10's UNKNOWN_TASK_TOKEN case: garbage bytes are indistinguishable from a
// token that was never issued, and both are NOT_FOUND.
func TestDecodeTaskTokenMalformed(t *testing.T) {
	t.Parallel()

	cases := map[string][]byte{
		"nil":     nil,
		"empty":   {},
		"garbage": {0xff, 0x00, 0x01, 0x02, 0xde, 0xad},
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := decodeTaskToken(b)
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.NotFound {
				t.Fatalf("decodeTaskToken(%v) = %v, want NotFound", b, err)
			}
		})
	}
}
