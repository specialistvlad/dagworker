package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dagworker "github.com/specialistvlad/dagworker"
	"github.com/specialistvlad/dagworker/storage/memory"
)

// flakyStore wraps the in-memory reference backend and lets a test flip
// Scopes between working and failing on demand, so /readyz's dependency on
// store reachability can be exercised without a real, sometimes-unreachable
// Redis or PostgreSQL. Every other [dagworker.Store] method is the real
// in-memory implementation's, reached through embedding.
type flakyStore struct {
	*memory.Store
	down atomic.Bool
}

func (f *flakyStore) Scopes(ctx context.Context) ([]dagworker.Scope, error) {
	if f.down.Load() {
		return nil, errors.New("flakyStore: store is down")
	}
	return f.Store.Scopes(ctx)
}

func newTestManager(t *testing.T, store dagworker.Store) *dagworker.Manager {
	t.Helper()
	mgr, err := dagworker.New(store, dagworker.WithoutBackgroundSweeper())
	if err != nil {
		t.Fatalf("dagworker.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mgr.Close(ctx)
	})
	return mgr
}

func TestReadyz_FailsWhileStoreUnreachableAndPassesOnceItIsNot(t *testing.T) {
	t.Parallel()

	fs := &flakyStore{Store: memory.New()}
	fs.down.Store(true)
	mgr := newTestManager(t, fs)

	var ready readiness
	handler := handleReadyz(mgr, &ready)

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != 503 {
		t.Errorf("readyz while store is down: status = %d, want 503", rec.Code)
	}

	fs.down.Store(false)
	req = httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != 200 {
		t.Errorf("readyz once store is reachable: status = %d, want 200", rec.Code)
	}
}

func TestReadyz_FailsImmediatelyOnceDraining(t *testing.T) {
	t.Parallel()

	fs := &flakyStore{Store: memory.New()}
	mgr := newTestManager(t, fs)

	var ready readiness
	handler := handleReadyz(mgr, &ready)

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("readyz before draining: status = %d, want 200", rec.Code)
	}

	ready.fail()

	req = httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	rec = httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != 503 {
		t.Errorf("readyz after ready.fail(): status = %d, want 503, even though the store itself is fine", rec.Code)
	}
}

// TestHealthz_PassesRegardlessOfStoreOrDrainingState pins down what makes
// liveness liveness: handleHealthz takes no store and no [*readiness] at
// all, so there is no state it could ever fail on, matching the requirement
// that /healthz must hold throughout a store outage and throughout the
// entire graceful-shutdown drain.
func TestHealthz_PassesRegardlessOfStoreOrDrainingState(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
	rec := httptest.NewRecorder()
	handleHealthz(rec, req)
	if rec.Code != 200 {
		t.Errorf("healthz status = %d, want 200", rec.Code)
	}
}

func TestHandleMetrics_ReportsDrainingState(t *testing.T) {
	t.Parallel()

	var ready readiness
	handler := handleMetrics(&ready, time.Now())

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "dagworkerd_draining 0") {
		t.Errorf("metrics body missing dagworkerd_draining 0 before draining:\n%s", rec.Body.String())
	}

	ready.fail()
	rec = httptest.NewRecorder()
	handler(rec, req)
	if !strings.Contains(rec.Body.String(), "dagworkerd_draining 1") {
		t.Errorf("metrics body missing dagworkerd_draining 1 after draining:\n%s", rec.Body.String())
	}
}
