package pq_test

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/specialistvlad/dagworker/internal/pq"
)

// keyed builds a heap ordered by an integer key held in keys, indexed by handle.
func keyed(keys []int64, capacity int) *pq.Heap {
	return pq.New(func(a, b int32) bool { return keys[a] < keys[b] }, capacity)
}

func TestPopsInOrder(t *testing.T) {
	t.Parallel()
	keys := []int64{5, 1, 4, 2, 3}
	h := keyed(keys, len(keys))
	for i := range keys {
		h.Push(int32(i))
	}
	var got []int64
	for {
		x, ok := h.Pop()
		if !ok {
			break
		}
		got = append(got, keys[x])
	}
	if want := []int64{1, 2, 3, 4, 5}; !slices.Equal(got, want) {
		t.Fatalf("popped %v, want %v", got, want)
	}
}

func TestEmpty(t *testing.T) {
	t.Parallel()
	h := keyed(nil, 0)
	if h.Len() != 0 {
		t.Fatalf("fresh heap has length %d", h.Len())
	}
	if _, ok := h.Pop(); ok {
		t.Fatal("Pop on an empty heap reported success")
	}
	if _, ok := h.Peek(); ok {
		t.Fatal("Peek on an empty heap reported success")
	}
	if h.Remove(7) {
		t.Fatal("Remove of an absent handle reported success")
	}
	if h.Fix(7) {
		t.Fatal("Fix of an absent handle reported success")
	}
	if h.Contains(7) || h.Contains(-1) {
		t.Fatal("empty heap claims to contain a handle")
	}
}

// The property that container/heap cannot offer, and the reason this type
// exists: an arbitrary element can be pulled out without a linear search.
func TestRemoveArbitrary(t *testing.T) {
	t.Parallel()
	keys := []int64{10, 20, 30, 40, 50, 60, 70}
	h := keyed(keys, len(keys))
	for i := range keys {
		h.Push(int32(i))
	}
	if !h.Remove(3) { // key 40, sitting in the middle
		t.Fatal("Remove of a present handle reported failure")
	}
	if h.Contains(3) {
		t.Fatal("removed handle is still reported present")
	}
	var got []int64
	for {
		x, ok := h.Pop()
		if !ok {
			break
		}
		got = append(got, keys[x])
	}
	if want := []int64{10, 20, 30, 50, 60, 70}; !slices.Equal(got, want) {
		t.Fatalf("popped %v, want %v", got, want)
	}
}

func TestFixAfterKeyChange(t *testing.T) {
	t.Parallel()
	keys := []int64{10, 20, 30}
	h := keyed(keys, len(keys))
	for i := range keys {
		h.Push(int32(i))
	}
	keys[2] = 1 // the last element becomes the smallest
	h.Fix(2)
	if x, _ := h.Peek(); x != 2 {
		t.Fatalf("after Fix the head is handle %d, want 2", x)
	}

	keys[2] = 99 // and now the largest
	h.Fix(2)
	if x, _ := h.Peek(); x != 0 {
		t.Fatalf("after the second Fix the head is handle %d, want 0", x)
	}
}

// Pushing a handle already present must reheapify rather than duplicate it,
// because the ready set pushes a node whose priority may have changed.
func TestPushExistingIsFix(t *testing.T) {
	t.Parallel()
	keys := []int64{10, 20, 30}
	h := keyed(keys, len(keys))
	for i := range keys {
		h.Push(int32(i))
	}
	keys[2] = 1
	h.Push(2)
	if h.Len() != 3 {
		t.Fatalf("re-pushing a present handle gave length %d, want 3", h.Len())
	}
	if x, _ := h.Peek(); x != 2 {
		t.Fatalf("head is handle %d, want 2", x)
	}
}

// The index grows on demand, so a handle far beyond the initial capacity hint
// must not panic or corrupt the mapping.
func TestGrowsBeyondCapacityHint(t *testing.T) {
	t.Parallel()
	keys := make([]int64, 500)
	for i := range keys {
		keys[i] = int64(500 - i)
	}
	h := keyed(keys, 4)
	for i := range keys {
		h.Push(int32(i))
	}
	if h.Len() != 500 {
		t.Fatalf("length is %d, want 500", h.Len())
	}
	last := int64(-1)
	for {
		x, ok := h.Pop()
		if !ok {
			break
		}
		if keys[x] < last {
			t.Fatalf("popped %d after %d, out of order", keys[x], last)
		}
		last = keys[x]
	}
}

// A randomised differential test against a plain sorted slice: whatever
// sequence of operations arrives, the heap must agree with the obvious model.
func TestAgreesWithSortedModel(t *testing.T) {
	t.Parallel()
	const n = 300
	rng := rand.New(rand.NewPCG(1, 2))

	keys := make([]int64, n)
	for i := range keys {
		keys[i] = rng.Int64N(1000)
	}
	h := keyed(keys, n)
	model := make(map[int32]bool)

	for range 4000 {
		switch rng.IntN(3) {
		case 0:
			x := int32(rng.IntN(n))
			h.Push(x)
			model[x] = true
		case 1:
			x := int32(rng.IntN(n))
			if h.Remove(x) != model[x] {
				t.Fatalf("Remove(%d) disagreed with the model", x)
			}
			delete(model, x)
		default:
			x, ok := h.Pop()
			if ok != (len(model) > 0) {
				t.Fatalf("Pop reported %v with %d elements in the model", ok, len(model))
			}
			if !ok {
				continue
			}
			if !model[x] {
				t.Fatalf("Pop returned handle %d, which the model does not hold", x)
			}
			for m := range model {
				if keys[m] < keys[x] {
					t.Fatalf("Pop returned key %d while the model holds a smaller %d", keys[x], keys[m])
				}
			}
			delete(model, x)
		}
		if h.Len() != len(model) {
			t.Fatalf("heap length %d, model holds %d", h.Len(), len(model))
		}
	}
}
