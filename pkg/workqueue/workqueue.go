package workqueue

import (
	"context"
	"errors"
	"sync"
)

var ErrInvalidLimit = errors.New("workqueue limit must be positive")

// State describes the lifecycle of one queued work item.
type State string

const (
	Queued       State = "queued"
	Admitted     State = "admitted"
	Active       State = "active"
	Completed    State = "completed"
	Cancelled    State = "cancelled"
	Dropped      State = "dropped"
	Acknowledged State = "acknowledged"
)

// Item is one opaque unit of work.
type Item[T any] struct {
	ID    string
	Value T
}

// Counts contains current queue gauges and cumulative lifecycle counters.
type Counts struct {
	Queued       int
	Admitted     int
	Active       int
	Completed    uint64
	Cancelled    uint64
	Dropped      uint64
	Acknowledged uint64
}

type entry[T any] struct {
	item    Item[T]
	state   State
	removed bool
}

// Queue models FIFO admission and exactly-once lifecycle accounting.
type Queue[T any] struct {
	mu      sync.Mutex
	changed chan struct{}
	limit   int
	active  int
	items   []*entry[T]
	counts  Counts
}

// Handle owns the lifecycle of one item.
type Handle[T any] struct {
	queue *Queue[T]
	entry *entry[T]
}

// Value returns the item owned by this handle.
func (h *Handle[T]) Value() T {
	return h.entry.item.Value
}

func New[T any](limit int) (*Queue[T], error) {
	if limit < 1 {
		return nil, ErrInvalidLimit
	}
	q := &Queue[T]{limit: limit, changed: make(chan struct{})}
	return q, nil
}

func (q *Queue[T]) Enqueue(item Item[T]) *Handle[T] {
	q.mu.Lock()
	defer q.mu.Unlock()
	entry := &entry[T]{item: item, state: Queued}
	q.items = append(q.items, entry)
	q.counts.Queued++
	q.signalLocked()
	return &Handle[T]{queue: q, entry: entry}
}

// Admit waits for this handle to reach the FIFO head and for capacity.
func (h *Handle[T]) Admit(ctx context.Context) error {
	q := h.queue
	for {
		q.mu.Lock()
		if h.entry.state == Cancelled || h.entry.state == Dropped {
			q.mu.Unlock()
			return context.Canceled
		}
		if h.entry.state != Queued {
			q.mu.Unlock()
			return nil
		}
		if q.canAdmitLocked(h.entry) {
			h.entry.state = Admitted
			q.counts.Queued--
			q.counts.Admitted++
			q.items = removeEntry(q.items, h.entry)
			q.signalLocked()
			q.mu.Unlock()
			return nil
		}
		if err := ctx.Err(); err != nil {
			h.entry.state = Cancelled
			q.counts.Queued--
			q.counts.Cancelled++
			q.items = removeEntry(q.items, h.entry)
			q.signalLocked()
			q.mu.Unlock()
			return err
		}
		changed := q.changed
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			q.mu.Lock()
			if h.entry.state == Queued {
				h.entry.state = Cancelled
				q.counts.Queued--
				q.counts.Cancelled++
				q.items = removeEntry(q.items, h.entry)
				q.signalLocked()
			}
			q.mu.Unlock()
			return ctx.Err()
		case <-changed:
		}
	}
}

func (q *Queue[T]) canAdmitLocked(target *entry[T]) bool {
	if q.active+q.counts.Admitted >= q.limit || len(q.items) == 0 {
		return false
	}
	return q.items[0] == target
}

func (h *Handle[T]) Start() bool {
	q := h.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	if h.entry.state != Admitted {
		return false
	}
	h.entry.state = Active
	q.counts.Admitted--
	q.counts.Active++
	q.active++
	return true
}

func (h *Handle[T]) Complete() bool { return h.finish(Completed) }
func (h *Handle[T]) Cancel() bool   { return h.finish(Cancelled) }
func (h *Handle[T]) Drop() bool     { return h.finish(Dropped) }

func (h *Handle[T]) finish(state State) bool {
	q := h.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	if h.entry.state != Active && h.entry.state != Admitted && h.entry.state != Queued {
		return false
	}
	if h.entry.state == Queued {
		q.counts.Queued--
		q.items = removeEntry(q.items, h.entry)
	} else if h.entry.state == Admitted {
		q.counts.Admitted--
	} else {
		q.counts.Active--
		q.active--
	}
	h.entry.state = state
	switch state {
	case Completed:
		q.counts.Completed++
	case Cancelled:
		q.counts.Cancelled++
	case Dropped:
		q.counts.Dropped++
	}
	q.signalLocked()
	return true
}

func (h *Handle[T]) Acknowledge() bool {
	q := h.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	if h.entry.state != Completed && h.entry.state != Cancelled && h.entry.state != Dropped {
		return false
	}
	h.entry.state = Acknowledged
	q.counts.Acknowledged++
	q.signalLocked()
	return true
}

// Release is idempotent and exists as an explicit cleanup marker for callers.
func (h *Handle[T]) Release() bool {
	q := h.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	if h.entry.removed {
		return false
	}
	h.entry.removed = true
	q.signalLocked()
	return true
}

func (q *Queue[T]) SetLimit(limit int) error {
	if limit < 1 {
		return ErrInvalidLimit
	}
	q.mu.Lock()
	q.limit = limit
	q.signalLocked()
	q.mu.Unlock()
	return nil
}

func (q *Queue[T]) Limit() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.limit
}

func (q *Queue[T]) Counts() Counts {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.counts
}

func (q *Queue[T]) signalLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}

func removeEntry[T any](items []*entry[T], target *entry[T]) []*entry[T] {
	for i, item := range items {
		if item == target {
			return append(items[:i], items[i+1:]...)
		}
	}
	return items
}
