package pipeline

import (
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

func sample(i int) model.Sample {
	return model.Sample{Name: "m", Timestamp: time.Unix(int64(i), 0), Value: float64(i)}
}

func TestQueueFIFOAndDropNewest(t *testing.T) {
	q := newBoundedQueue(3, PolicyDropNewest)
	for i := 0; i < 3; i++ {
		if err := q.Push(sample(i)); err != nil {
			t.Fatal(err)
		}
	}
	err := q.Push(sample(99))
	if re, ok := err.(*model.RejectError); !ok || re.Reason != model.RejectQueueFull {
		t.Fatalf("want queue_full reject, got %v", err)
	}
	for i := 0; i < 3; i++ {
		s, ok := q.Pop()
		if !ok || s.Value != float64(i) {
			t.Fatalf("FIFO broken at %d: %v %v", i, s, ok)
		}
	}
}

func TestQueueDropOldest(t *testing.T) {
	q := newBoundedQueue(2, PolicyDropOldest)
	_ = q.Push(sample(1))
	_ = q.Push(sample(2))
	if err := q.Push(sample(3)); err != nil {
		t.Fatal(err)
	}
	if q.DroppedCount() != 1 {
		t.Fatalf("dropped = %d, want 1", q.DroppedCount())
	}
	s, ok := q.Pop()
	if !ok || s.Value != 2 {
		t.Fatalf("want oldest kept (2), got %v", s.Value)
	}
}

func TestQueueCloseDrainsAll(t *testing.T) {
	q := newBoundedQueue(10, PolicyBlock)
	var mu sync.Mutex
	var got []int
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			s, ok := q.Pop()
			if !ok {
				return
			}
			mu.Lock()
			got = append(got, int(s.Value))
			mu.Unlock()
		}
	}()
	for i := 0; i < 10; i++ {
		_ = q.Push(sample(i))
	}
	q.Close()
	wg.Wait()
	if len(got) != 10 {
		t.Fatalf("drained %d items, want 10 (no half loss)", len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("order broken at %d: %d", i, v)
		}
	}
	if err := q.Push(sample(1)); err == nil {
		t.Fatal("push after close accepted")
	}
}

func TestQueueBlockThenCloseWakesWaiters(t *testing.T) {
	q := newBoundedQueue(1, PolicyBlock)
	_ = q.Push(sample(0))
	done := make(chan error, 1)
	go func() { done <- q.Push(sample(1)) }()
	time.Sleep(20 * time.Millisecond)
	go func() {
		q.Pop()
		q.Close()
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("push should fail once closed while waiting")
		}
	case <-time.After(time.Second):
		t.Fatal("blocked push never woke on close")
	}
}
