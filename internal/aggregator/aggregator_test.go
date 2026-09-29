package aggregator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

func baseTime() time.Time {
	return time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)
}

// drain stops the watermark ticker, flushes all buffered windows via Close
// and collects everything emitted until the output channel is closed.
func drain(t *testing.T, a *Aggregator) []model.Aggregate {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ch := a.Run(ctx)
	cancel()
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	var out []model.Aggregate
	for g := range ch {
		out = append(out, g)
	}
	return out
}

func TestOutOfOrderAndDuplicates(t *testing.T) {
	t0 := baseTime()
	a := New(Config{Window: time.Minute, MaxSeries: 10})

	samples := []model.Sample{
		{Name: "cpu", Labels: map[string]string{"host": "h1"}, Value: 10, Timestamp: t0.Add(30 * time.Second)},
		{Name: "cpu", Labels: map[string]string{"host": "h1"}, Value: 50, Timestamp: t0.Add(10 * time.Second)}, // out of order
		{Name: "cpu", Labels: map[string]string{"host": "h1"}, Value: 90, Timestamp: t0.Add(50 * time.Second)},
		{Name: "cpu", Labels: map[string]string{"host": "h1"}, Value: 90, Timestamp: t0.Add(50 * time.Second)}, // exact dup
		{Name: "cpu", Labels: map[string]string{"host": "h1"}, Value: 99, Timestamp: t0.Add(50 * time.Second)}, // conflicting dup: first wins
		{Name: "cpu", Labels: map[string]string{"host": "h1"}, Value: 5, Timestamp: t0.Add(20 * time.Second)},
	}
	for _, s := range samples {
		if err := a.Ingest(s); err != nil {
			t.Fatalf("ingest: %v", err)
		}
	}

	out := drain(t, a)
	if len(out) != 1 {
		t.Fatalf("want 1 window, got %d: %+v", len(out), out)
	}
	g := out[0]
	if g.Count != 4 {
		t.Fatalf("want count 4 (2 dup reports collapsed), got %d", g.Count)
	}
	if g.Sum != 155 { // 10+50+90+5
		t.Fatalf("want sum 155, got %v", g.Sum)
	}
	if g.Min != 5 || g.Max != 90 {
		t.Fatalf("want min 5 max 90, got min %v max %v", g.Min, g.Max)
	}
	if g.Last != 90 { // latest timestamp 00:50 -> first value 90
		t.Fatalf("want last 90 (latest timestamp wins, not insertion order), got %v", g.Last)
	}
}

func TestConcurrentWritesSameSeries(t *testing.T) {
	t0 := baseTime()
	a := New(Config{Window: time.Hour, MaxSeries: 10})

	const goroutines = 16
	const per = 200
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				ts := t0.Add(time.Duration(i) * time.Second)
				s := model.Sample{
					Name:      "cpu",
					Labels:    map[string]string{"host": "h1"},
					Value:     1,
					Timestamp: ts,
				}
				if err := a.Ingest(s); err != nil {
					errCh <- err
					return
				}
				// duplicate report from any goroutine must be harmless
				if err := a.Ingest(s); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("ingest error: %v", err)
	}

	out := drain(t, a)
	if len(out) != 1 {
		t.Fatalf("want 1 window, got %d", len(out))
	}
	if out[0].Count != per {
		t.Fatalf("want exactly %d distinct samples under concurrency, got %d", per, out[0].Count)
	}
	if out[0].Sum != float64(per) {
		t.Fatalf("want sum %d, got %v", per, out[0].Sum)
	}
}

func TestLabelIsolation(t *testing.T) {
	t0 := baseTime()
	a := New(Config{Window: time.Minute, MaxSeries: 10})
	_ = a.Ingest(model.Sample{Name: "cpu", Labels: map[string]string{"host": "a"}, Value: 1, Timestamp: t0})
	_ = a.Ingest(model.Sample{Name: "cpu", Labels: map[string]string{"host": "b"}, Value: 9, Timestamp: t0})
	_ = a.Ingest(model.Sample{Name: "mem", Labels: map[string]string{"host": "a"}, Value: 3, Timestamp: t0})

	out := drain(t, a)
	if len(out) != 3 {
		t.Fatalf("want 3 series windows, got %d", len(out))
	}
	got := map[string]float64{}
	for _, g := range out {
		got[model.Fingerprint(g.Name, g.Labels)] = g.Sum
	}
	if got["cpu{host=a}"] != 1 || got["cpu{host=b}"] != 9 || got["mem{host=a}"] != 3 {
		t.Fatalf("label isolation broken: %v", got)
	}
}

func TestCardinalityRejection(t *testing.T) {
	t0 := baseTime()
	a := New(Config{Window: time.Minute, MaxSeries: 2})
	mustIngest := func(host string) {
		t.Helper()
		err := a.Ingest(model.Sample{Name: "cpu", Labels: map[string]string{"host": host}, Value: 1, Timestamp: t0})
		if err != nil {
			t.Fatalf("unexpected reject for %s: %v", host, err)
		}
	}
	mustIngest("a")
	mustIngest("b")
	err := a.Ingest(model.Sample{Name: "cpu", Labels: map[string]string{"host": "c"}, Value: 1, Timestamp: t0})
	var rerr *RejectError
	if !errors.As(err, &rerr) || rerr.Reason != RejectCardinality {
		t.Fatalf("want cardinality rejection, got %v", err)
	}
	t.Logf("rejection reason: %s detail: %s", rerr.Reason, rerr.Detail)

	// existing series still accepted and the rejected series never counted
	if err := a.Ingest(model.Sample{Name: "cpu", Labels: map[string]string{"host": "a"}, Value: 4, Timestamp: t0.Add(time.Second)}); err != nil {
		t.Fatalf("existing series rejected: %v", err)
	}
	out := drain(t, a)
	if len(out) != 2 {
		t.Fatalf("want 2 windows (rejected series not admitted), got %d", len(out))
	}
}

func TestWatermarkEmitsAndRejectsLate(t *testing.T) {
	w := 20 * time.Millisecond
	a := New(Config{Window: w, MaxSeries: 10, WatermarkLag: w})
	ctx, cancel := context.WithCancel(context.Background())
	out := a.Run(ctx)

	start := time.Now().Truncate(w)
	if err := a.Ingest(model.Sample{Name: "cpu", Labels: nil, Value: 7, Timestamp: start.Add(5 * time.Millisecond)}); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	var agg model.Aggregate
	select {
	case agg = <-out:
	case <-time.After(2 * time.Second):
		t.Fatal("window never emitted")
	}
	if agg.Count != 1 || agg.Sum != 7 {
		t.Fatalf("unexpected agg: %+v", agg)
	}

	// After the window is emitted, a late sample for it must be rejected
	// with an explicit reason rather than silently corrupting statistics.
	err := a.Ingest(model.Sample{Name: "cpu", Labels: nil, Value: 100, Timestamp: start.Add(6 * time.Millisecond)})
	var rerr *RejectError
	if !errors.As(err, &rerr) || rerr.Reason != RejectLate {
		t.Fatalf("want late rejection, got %v", err)
	}

	cancel()
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestCloseFlushesEveryWindow(t *testing.T) {
	t0 := baseTime()
	a := New(Config{Window: time.Minute, MaxSeries: 10})
	for i := 0; i < 3; i++ {
		_ = a.Ingest(model.Sample{Name: "cpu", Labels: map[string]string{"host": "h"},
			Value: float64(i + 1), Timestamp: t0.Add(time.Duration(i) * time.Minute)})
	}
	out := drain(t, a)
	if len(out) != 3 {
		t.Fatalf("shutdown must flush all buffered windows, want 3 got %d", len(out))
	}
	for i := 0; i < 3; i++ {
		if !out[i].WindowStart.Equal(t0.Add(time.Duration(i) * time.Minute)) {
			t.Fatalf("flush must be ordered oldest-first, got %v at index %d", out[i].WindowStart, i)
		}
	}
}
