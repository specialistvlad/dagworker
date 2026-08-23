package dagworker_test

import (
	"context"
	"errors"
	"testing"

	dw "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
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
	if _, err := tv.Nack(f.ctx, lease, errors.New("nope")); err != nil {
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

// TestTypedPoisonNodeIsNotRetried pins down what Typed[T].Claim's own doc
// comment promises: a payload that cannot be decoded fails identically on
// every attempt, so retrying it just burns three workers' time to reach the
// same conclusion and leaves the graph blocked for longer.
//
// The doc said "failed immediately rather than retried" while the code called
// Nack, which retries per the scope's policy — three attempts by default.
func TestTypedPoisonNodeIsNotRetried(t *testing.T) {
	t.Parallel()

	type payload struct {
		N int `json:"n"`
	}
	st := memory.New()
	m, err := dw.New(st, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	ctx := t.Context()

	// The scope's default retry policy, which is what makes this a test rather
	// than a tautology: three attempts, so a Nack would leave the node ready.
	if err := m.Configure(ctx, "s", dw.ScopeConfig{MaxAttempts: 3}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if err := m.AddNode(ctx, "s", "poison", []byte(`{"n": "not a number"}`)); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	typed := dw.NewTyped[payload](m, "s")
	if _, err := typed.TryClaim(ctx); err == nil {
		t.Fatal("an undecodable payload was claimed successfully")
	} else if !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("a decode failure does not unwrap to any sentinel: %v", err)
	}

	n, err := m.GetNode(ctx, "s", "poison")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if n.Status != dw.StatusError {
		t.Fatalf("the poison node is %v after a failed decode, want a terminal error", n.Status)
	}

	// And it must stay that way: nothing should hand it to a second worker.
	if _, err := typed.TryClaim(ctx); !errors.Is(err, dw.ErrNoWork) {
		t.Fatalf("the poison node was offered again: %v", err)
	}
}

// TestTypedErrorsWrapSentinels: every error this wrapper invents must be
// classifiable with errors.Is, like every other error in the library. A
// convenience wrapper that returns errors nobody can branch on makes the
// caller parse strings.
func TestTypedErrorsWrapSentinels(t *testing.T) {
	t.Parallel()

	st := memory.New()
	m, err := dw.New(st, dw.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = st.Close(context.Background())
	})
	ctx := t.Context()

	// A channel is not encodable as JSON.
	type unencodable struct {
		C chan int `json:"c"`
	}
	bad := dw.NewTyped[unencodable](m, "s")
	if err := bad.AddNode(ctx, "n", unencodable{C: make(chan int)}); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("AddNode with an unencodable payload gave %v", err)
	}

	// An unencodable *result*, reported through Ack.
	type ok struct {
		N int `json:"n"`
	}
	good := dw.NewTyped[ok](m, "s")
	if err := good.AddNode(ctx, "fine", ok{N: 1}); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	lease, err := good.TryClaim(ctx)
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if err := good.Ack(ctx, lease, make(chan int)); !errors.Is(err, dw.ErrInvalidArgument) {
		t.Fatalf("Ack with an unencodable result gave %v", err)
	}
}
