package store

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

func rejectReason(t *testing.T, err error) model.RejectReason {
	t.Helper()
	if err == nil {
		t.Fatalf("expected reject error")
	}
	re, ok := err.(*model.RejectError)
	if !ok {
		t.Fatalf("expected *RejectError, got %T", err)
	}
	return re.Reason
}

func TestConcurrentSameSeriesAndDuplicates(t *testing.T) {
	s := New(&Options{WindowSize: time.Second, MaxSeries: 10, StaleWindows: 2})
	base := time.Unix(100, 0)
	labels := map[string]string{"host": "a"}

	var wg sync.WaitGroup
	// 50 goroutines writing the same 5 timestamps with the same value;
	// identical timestamps are duplicate reports.
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				if err := s.Ingest(model.Sample{
					Name: "cpu", Labels: labels, Value: 1,
					Timestamp: base.Add(time.Duration(i) * 100 * time.Millisecond),
				}); err != nil {
					t.Errorf("ingest: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	if s.SeriesCount() != 1 {
		t.Fatalf("series count = %d, want 1", s.SeriesCount())
	}
	// Flush well after the window is ready: window [100,101),
	// readyAt = 103.
	aggs := s.Flush(time.Unix(104, 0))
	if len(aggs) != 1 {
		t.Fatalf("aggs = %d, want 1", len(aggs))
	}
	a := aggs[0]
	if a.Count != 5 {
		t.Fatalf("count = %d, want 5 (duplicates merged)", a.Count)
	}
	if a.Sum != 5 || a.Min != 1 || a.Max != 1 || a.Latest != 1 {
		t.Fatalf("bad aggregate: %+v", a)
	}
}

func TestDuplicateLastWriteWins(t *testing.T) {
	s := New(&Options{WindowSize: time.Second, MaxSeries: 10})
	ts := time.Unix(100, 10)
	_ = s.Ingest(model.Sample{Name: "m", Timestamp: ts, Value: 1})
	_ = s.Ingest(model.Sample{Name: "m", Timestamp: ts, Value: 9})
	aggs := s.Flush(time.Unix(103, 0))
	if len(aggs) != 1 || aggs[0].Count != 1 || aggs[0].Sum != 9 || aggs[0].Latest != 9 {
		t.Fatalf("duplicate replacement failed: %+v", aggs)
	}
}

func TestOutOfOrderAcceptedAndStaleRejected(t *testing.T) {
	s := New(&Options{WindowSize: time.Second, MaxSeries: 10, StaleWindows: 2})
	// Establish newest window at t=100.
	_ = s.Ingest(model.Sample{Name: "m", Timestamp: time.Unix(100, 5), Value: 1})
	// One window late is still in the bound.
	if err := s.Ingest(model.Sample{Name: "m", Timestamp: time.Unix(99, 5), Value: 2}); err != nil {
		t.Fatalf("late-in-bound rejected: %v", err)
	}
	// Three windows late is stale.
	err := s.Ingest(model.Sample{Name: "m", Timestamp: time.Unix(97, 5), Value: 3})
	if r := rejectReason(t, err); r != model.RejectStale {
		t.Fatalf("reason = %s, want stale", r)
	}
	aggs := s.Flush(time.Unix(103, 0))
	byStart := map[int64]model.WindowAggregate{}
	for _, a := range aggs {
		byStart[a.WindowStart.Unix()] = a
	}
	if a := byStart[99]; a.Count != 1 || a.Sum != 2 {
		t.Fatalf("out-of-order window wrong: %+v", byStart)
	}
}

func TestCardinalityLimit(t *testing.T) {
	s := New(&Options{WindowSize: time.Second, MaxSeries: 3})
	for i := 0; i < 3; i++ {
		if err := s.Ingest(model.Sample{
			Name: "m", Labels: map[string]string{"id": string(rune('a' + i))},
			Timestamp: time.Unix(100, int64(i)),
		}); err != nil {
			t.Fatalf("unexpected reject: %v", err)
		}
	}
	err := s.Ingest(model.Sample{
		Name: "m", Labels: map[string]string{"id": "z"},
		Timestamp: time.Unix(100, 9),
	})
	re, ok := err.(*model.RejectError)
	if !ok || re.Reason != model.RejectCardinality {
		t.Fatalf("want cardinality reject, got %v", err)
	}
	if !strings.Contains(re.Error(), "cardinality_limit_exceeded") {
		t.Fatalf("reason not explainable: %v", err)
	}
	// Existing series still writable.
	if err := s.Ingest(model.Sample{Name: "m", Labels: map[string]string{"id": "a"},
		Timestamp: time.Unix(100, 50), Value: 2}); err != nil {
		t.Fatalf("existing series write failed: %v", err)
	}
}

func TestInvalidSample(t *testing.T) {
	s := nilStore()
	if err := s.Ingest(model.Sample{Timestamp: time.Now()}); err == nil {
		t.Fatal("empty name accepted")
	}
	if err := s.Ingest(model.Sample{Name: "m"}); err == nil {
		t.Fatal("zero ts accepted")
	}
}

func nilStore() *Store { return New(nil) }
