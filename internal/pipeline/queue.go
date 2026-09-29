package pipeline

import (
	"strconv"
	"sync"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// OverflowPolicy decides what happens when the bounded ingest queue is
// full.
type OverflowPolicy int

const (
	// PolicyBlock makes Submit wait until a slot frees. Nothing is
	// dropped; it is the backpressure policy.
	PolicyBlock OverflowPolicy = iota
	// PolicyDropNewest rejects the incoming sample immediately with
	// RejectQueueFull.
	PolicyDropNewest
	// PolicyDropOldest evicts the oldest queued sample (reported via
	// Dropped) and enqueues the new one.
	PolicyDropOldest
)

func (p OverflowPolicy) String() string {
	switch p {
	case PolicyDropNewest:
		return "drop_newest"
	case PolicyDropOldest:
		return "drop_oldest"
	default:
		return "block"
	}
}

// queued samples the ring slot and its presence.
type queued struct {
	sample model.Sample
	used   bool
}

// boundedQueue is a fixed-capacity FIFO with explicit overflow policy.
// Unlike a Go channel it lets the DropOldest policy evict an item and
// lets Close guarantee every item still present is popped exactly once
// ("queued data is not dropped in half").
type boundedQueue struct {
	capacity int
	policy   OverflowPolicy

	mu       sync.Mutex
	ring     []queued
	head     int // index of oldest item
	size     int
	closed   bool
	notEmpty *sync.Cond
	notFull  *sync.Cond

	// dropFromQueue counts evictions caused by DropOldest only.
	dropMu       sync.Mutex
	droppedCount int64
}

func newBoundedQueue(capacity int, policy OverflowPolicy) *boundedQueue {
	if capacity <= 0 {
		capacity = 1
	}
	q := &boundedQueue{
		capacity: capacity,
		policy:   policy,
		ring:     make([]queued, capacity),
	}
	q.notEmpty = sync.NewCond(&q.mu)
	q.notFull = sync.NewCond(&q.mu)
	return q
}

// Push adds a sample. Under PolicyBlock it blocks while full or until
// Close; under drop policies it returns *model.RejectError(RejectQueueFull)
// when a new sample cannot be admitted. A push after Close fails too.
func (q *boundedQueue) Push(s model.Sample) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.size == q.capacity && !q.closed && q.policy == PolicyBlock {
		q.notFull.Wait()
	}
	if q.closed {
		return &model.RejectError{Reason: model.RejectQueueFull, Detail: "pipeline stopped"}
	}
	if q.size == q.capacity {
		switch q.policy {
		case PolicyDropNewest:
			return &model.RejectError{
				Reason: model.RejectQueueFull,
				Detail: "ingest queue full (" + strconv.Itoa(q.capacity) + "); policy=drop_newest",
			}
		case PolicyDropOldest:
			q.ring[q.head].used = false
			q.head = (q.head + 1) % q.capacity
			q.size--
			q.addDropped(1)
		}
	}

	tail := (q.head + q.size) % q.capacity
	q.ring[tail] = queued{sample: s, used: true}
	q.size++
	q.notEmpty.Signal()
	return nil
}

// Pop removes the oldest sample. It blocks while empty and returns
// false only after Close has fully drained the queue.
func (q *boundedQueue) Pop() (model.Sample, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.size == 0 && !q.closed {
		q.notEmpty.Wait()
	}
	if q.size == 0 {
		return model.Sample{}, false
	}
	item := q.ring[q.head]
	q.ring[q.head] = queued{}
	q.head = (q.head + 1) % q.capacity
	q.size--
	if q.size == 0 && q.closed {
		// Wake every blocked Popper so they observe drained state.
		q.notEmpty.Broadcast()
	}
	q.notFull.Signal()
	return item.sample, true
}

// Close marks the queue closed; already queued items remain available to
// Pop. It is safe to call once.
func (q *boundedQueue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		q.notEmpty.Broadcast()
		q.notFull.Broadcast()
	}
}

// Len returns the current queue length.
func (q *boundedQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.size
}

func (q *boundedQueue) addDropped(n int64) {
	q.dropMu.Lock()
	q.droppedCount += n
	q.dropMu.Unlock()
}

// DroppedCount returns the number of DropOldest evictions.
func (q *boundedQueue) DroppedCount() int64 {
	q.dropMu.Lock()
	defer q.dropMu.Unlock()
	return q.droppedCount
}
