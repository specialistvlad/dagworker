package dagworker_test

import (
	"errors"
	"testing"

	dw "github.com/specialistvlad/dagworker"
)

type job struct {
	URL   string `json:"url"`
	Depth int    `json:"depth"`
}

func TestTypedRoundTrip(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	tv := dw.NewTyped[job](f.m, "s")

	if tv.Scope() != "s" || tv.Manager() != f.m {
		t.Fatal("the typed view does not report what it was built with")
	}

	want := job{URL: "https://example.test", Depth: 3}
	if err := tv.AddNode(f.ctx, "crawl", want, dw.WithKind("http")); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	lease, err := tv.TryClaim(f.ctx, dw.OfKind("http"))
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if lease.Payload != want {
		t.Fatalf("decoded %+v, want %+v", lease.Payload, want)
	}

	if err := tv.Ack(f.ctx, lease, map[string]int{"found": 12}); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	node, payload, err := tv.GetNode(f.ctx, "crawl")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if node.Status != dw.StatusSuccess || payload != want {
		t.Fatalf("after Ack the node is %v with payload %+v", node.Status, payload)
	}
}

func TestTypedClaimBlocks(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	tv := dw.NewTyped[job](f.m, "s")
	if err := tv.AddNode(f.ctx, "a", job{URL: "u"}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	lease, err := tv.Claim(f.ctx)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if lease.Payload.URL != "u" {
		t.Fatalf("payload is %+v", lease.Payload)
	}
	if err := tv.Nack(f.ctx, lease, errors.New("nope")); err != nil {
		t.Fatalf("Nack: %v", err)
	}
}

// A payload that cannot be decoded would fail identically on every attempt, so
// it is failed at once rather than retried forever.
func TestTypedRejectsAnUndecodablePayload(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	if err := f.m.Configure(f.ctx, "s", dw.ScopeConfig{MaxAttempts: 1}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	// Written as raw bytes that are not valid JSON for the target type.
	if err := f.m.AddNode(f.ctx, "s", "bad", []byte(`{"depth":"not a number"}`)); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	tv := dw.NewTyped[job](f.m, "s")
	if _, err := tv.TryClaim(f.ctx); err == nil {
		t.Fatal("an undecodable payload was accepted")
	}

	n, err := f.m.GetNode(f.ctx, "s", "bad")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if n.Status != dw.StatusError {
		t.Fatalf("a poison node is %v, want it failed rather than left to retry", n.Status)
	}
}

func TestTypedPropagatesNoWork(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	tv := dw.NewTyped[job](f.m, "s")
	if _, err := tv.TryClaim(f.ctx); !errors.Is(err, dw.ErrNoWork) {
		t.Fatalf("claiming an empty scope gave %v, want ErrNoWork", err)
	}
	if _, _, err := tv.GetNode(f.ctx, "missing"); !errors.Is(err, dw.ErrNotFound) {
		t.Fatalf("reading a missing node gave %v", err)
	}
}

func TestTypedRejectsUnencodablePayload(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	tv := dw.NewTyped[chan int](f.m, "s")
	if err := tv.AddNode(f.ctx, "a", make(chan int)); err == nil {
		t.Fatal("a payload that cannot be marshalled was accepted")
	}
}
