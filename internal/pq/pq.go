// Package pq provides an indexed binary heap over dense int32 handles.
//
// It differs from container/heap in the one way that matters for a scheduler:
// an arbitrary element can be located and removed in O(log n), because the heap
// maintains a handle-to-position index. A ready set needs that — adding an edge
// to a node that was already claimable must pull it back out of the queue — and
// container/heap's interface cannot express it without a linear search.
//
// The index is a slice rather than a map because handles are dense: they are
// allocated sequentially from a free list, so a slice indexed by handle is
// smaller and faster than a hash map, and never has to rehash.
package pq

// Less reports whether the element with handle a sorts before the element with
// handle b. It is called with handles, not values, so the caller keeps its data
// wherever it likes — typically parallel arrays indexed by the same handle.
type Less func(a, b int32) bool

// Heap is an indexed min-heap of int32 handles. The zero value is not usable;
// call [New].
type Heap struct {
	items []int32
	// pos maps a handle to its index in items, biased by one so that the zero
	// value means "not present" and no separate presence bitmap is needed.
	pos  []int32
	less Less
}

// New returns an empty heap ordered by less, sized for handles below capacity.
// The heap grows on demand, so capacity is a hint, not a limit.
func New(less Less, capacity int) *Heap {
	if capacity < 0 {
		capacity = 0
	}
	return &Heap{
		items: make([]int32, 0, capacity),
		pos:   make([]int32, capacity),
		less:  less,
	}
}

// Len returns the number of handles in the heap.
func (h *Heap) Len() int { return len(h.items) }

// Contains reports whether the handle is in the heap.
func (h *Heap) Contains(handle int32) bool {
	return handle >= 0 && int(handle) < len(h.pos) && h.pos[handle] != 0
}

func (h *Heap) ensure(handle int32) {
	if int(handle) < len(h.pos) {
		return
	}
	grown := make([]int32, max(int(handle)+1, 2*len(h.pos)+1))
	copy(grown, h.pos)
	h.pos = grown
}

// Push adds a handle. Pushing a handle already present is a no-op followed by a
// reheapify, which is the correct response to "this element's key changed".
func (h *Heap) Push(handle int32) {
	h.ensure(handle)
	if h.pos[handle] != 0 {
		h.Fix(handle)
		return
	}
	h.items = append(h.items, handle)
	// Safe by construction: items holds distinct int32 handles, so its length
	// can never exceed the int32 range.
	h.pos[handle] = int32(len(h.items)) //nolint:gosec // bounded by the handle space
	h.up(len(h.items) - 1)
}

// Peek returns the minimum handle without removing it. The second result is
// false when the heap is empty.
func (h *Heap) Peek() (int32, bool) {
	if len(h.items) == 0 {
		return 0, false
	}
	return h.items[0], true
}

// Pop removes and returns the minimum handle. The second result is false when
// the heap is empty.
func (h *Heap) Pop() (int32, bool) {
	if len(h.items) == 0 {
		return 0, false
	}
	top := h.items[0]
	h.swap(0, len(h.items)-1)
	h.items = h.items[:len(h.items)-1]
	h.pos[top] = 0
	if len(h.items) > 0 {
		h.down(0)
	}
	return top, true
}

// Remove takes a handle out of the heap wherever it sits. It reports whether
// the handle was present.
func (h *Heap) Remove(handle int32) bool {
	if !h.Contains(handle) {
		return false
	}
	i := int(h.pos[handle] - 1)
	last := len(h.items) - 1
	h.swap(i, last)
	h.items = h.items[:last]
	h.pos[handle] = 0
	if i < last {
		if !h.down(i) {
			h.up(i)
		}
	}
	return true
}

// Fix restores the heap invariant after the key of an element already in the
// heap has changed. It reports whether the handle was present.
func (h *Heap) Fix(handle int32) bool {
	if !h.Contains(handle) {
		return false
	}
	i := int(h.pos[handle] - 1)
	if !h.down(i) {
		h.up(i)
	}
	return true
}

func (h *Heap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	// Both indices are below len(items), which is bounded by the int32 handle
	// space, so neither conversion can overflow.
	h.pos[h.items[i]] = int32(i + 1) //nolint:gosec // bounded by the handle space
	h.pos[h.items[j]] = int32(j + 1) //nolint:gosec // bounded by the handle space
}

func (h *Heap) up(j int) {
	for j > 0 {
		parent := (j - 1) / 2
		if !h.less(h.items[j], h.items[parent]) {
			break
		}
		h.swap(parent, j)
		j = parent
	}
}

// down sifts the element at i downwards and reports whether it moved.
func (h *Heap) down(i0 int) bool {
	n := len(h.items)
	i := i0
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		smallest := left
		if right := left + 1; right < n && h.less(h.items[right], h.items[left]) {
			smallest = right
		}
		if !h.less(h.items[smallest], h.items[i]) {
			break
		}
		h.swap(i, smallest)
		i = smallest
	}
	return i > i0
}
