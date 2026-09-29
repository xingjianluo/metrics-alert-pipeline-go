package pipeline

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// detConfig builds a pipeline config with the background ticker
// effectively disabled, so tests deterministically drive evaluateOnce
// at synthetic times after the queue is drained.
func detConfig(t *testing.T, rs rule.Snapshot, mutate func(*Config)) Config {
	t.Helper()
	cfg := Config{
		Store: storeOpts(), QueueSize: 256, Policy: PolicyBlock,
		IngestWorker: 2, FlushInterval: time.Hour,
		Ruleset: rs, Logger: testLogger(),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return cfg
}

func baseRules() rule.Snapshot {
	return rule.Snapshot{Version: 1, Rules: []rule.Rule{{
		ID: "hot_cpu", Metric: "cpu", Field: rule.FieldAvg, Op: rule.OpGT,
		Threshold: 5, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second,
	}}}
}

func TestEndToEndFiringAndResolve(t *testing.T) {
	p, err := New(detConfig(t, baseRules(), nil))
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	defer func() { _ = p.Stop(context.Background()) }()

	var processed int64
	step := func(sec int64, v float64) {
		if err := p.Submit(model.Sample{Name: "cpu",
			Labels: map[string]string{"host": "a"}, Value: v,
			Timestamp: time.Unix(sec, 0)}); err != nil {
			t.Fatal(err)
		}
		processed++
		p.drainAt(t, processed, time.Unix(sec+3, 0))
	}
	for _, s := range []int64{0, 1, 2} {
		step(s, 9)
	}
	waitFor(t, p, func() bool { return p.Stats().FiringEmitted == 1 })
	for _, s := range []int64{3, 4, 5} {
		step(s, 1)
	}
	waitFor(t, p, func() bool { return p.Stats().ResolvedEmitted == 1 })
}

func TestConcurrentWritesStableResults(t *testing.T) {
	cfg := detConfig(t, baseRules(), nil)
	cfg.IngestWorker = 4
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.Start()

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				ts := time.Unix(0, int64(i)*100_000_000) // window [0,1)
				if err := p.Submit(model.Sample{Name: "cpu",
					Labels: map[string]string{"host": "a"}, Value: 1, Timestamp: ts}); err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
	waitFor(t, p, func() bool { return p.Stats().Accepted == 200 })

	// Duplicate timestamps are merged inside the store: the flushed
	// window must contain exactly the 10 distinct timestamps.
	aggs := p.Store().Flush(time.Unix(3, 0))
	if len(aggs) != 1 || aggs[0].Count != 10 {
		t.Fatalf("agg = %+v, want one window with count 10", aggs)
	}
	if aggs[0].Sum != 10 {
		t.Fatalf("sum = %v, want 10", aggs[0].Sum)
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.Stats().Rejected != 0 {
		t.Fatalf("rejected = %d, want 0", p.Stats().Rejected)
	}
}

func TestCardinalityRejectReason(t *testing.T) {
	cfg := detConfig(t, baseRules(), func(c *Config) { c.Policy = PolicyDropNewest })
	cfg.Store.MaxSeries = 3
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	defer func() { _ = p.Stop(context.Background()) }()

	var processed int64
	for i := 0; i < 4; i++ {
		err := p.Submit(model.Sample{Name: "cpu",
			Labels:    map[string]string{"id": string(rune('a' + i))},
			Timestamp: time.Unix(0, int64(i))})
		if err != nil { // drop_newest rejects at queue level; also counted
			processed++
		}
	}
	// The 4 samples that were queued are all processed too.
	waitFor(t, p, func() bool { return p.Stats().RejectedBy[model.RejectCardinality] >= 1 })
	_ = processed
}

func TestHotUpdateDuringEvaluation(t *testing.T) {
	p, err := New(detConfig(t, baseRules(), nil))
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	defer func() { _ = p.Stop(context.Background()) }()

	var processed int64
	step := func(sec int64, v float64) {
		_ = p.Submit(model.Sample{Name: "cpu", Labels: map[string]string{"host": "a"},
			Value: v, Timestamp: time.Unix(sec, 0)})
		processed++
		p.drainAt(t, processed, time.Unix(sec+3, 0))
	}
	step(0, 9) // pending

	// Concurrent hot updates, same rule ID, changed threshold.
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := baseRules().Rules[0]
			r.Threshold = 2
			_ = p.UpdateRules(rule.Snapshot{Version: 2, Rules: []rule.Rule{r}})
		}()
	}
	wg.Wait()

	step(1, 9)
	step(2, 9)
	waitFor(t, p, func() bool { return p.Stats().FiringEmitted >= 1 })
	if got := p.Stats().FiringEmitted; got != 1 {
		t.Fatalf("firing events = %d, want exactly 1 (no double counting)", got)
	}
}

func TestFlapDeduplicatesViaSink(t *testing.T) {
	var mu sync.Mutex
	var firing, resolved int
	cfg := detConfig(t, baseRules(), func(c *Config) {
		c.EventSink = func(ev model.AlertEvent) {
			mu.Lock()
			defer mu.Unlock()
			if ev.State == model.StateFiring {
				firing++
			} else {
				resolved++
			}
		}
	})
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	defer func() { _ = p.Stop(context.Background()) }()

	var processed int64
	step := func(sec int64, v float64) {
		_ = p.Submit(model.Sample{Name: "cpu", Labels: map[string]string{"host": "a"},
			Value: v, Timestamp: time.Unix(sec, 0)})
		processed++
		p.drainAt(t, processed, time.Unix(sec+3, 0))
	}
	for _, s := range []int64{0, 1, 2} {
		step(s, 9) // fire at s=2
	}
	step(3, 1) // resolving
	step(4, 9) // flap back: no new event
	for _, s := range []int64{5, 6, 7} {
		step(s, 1) // resolve at s=7
	}
	waitFor(t, p, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return firing == 1 && resolved == 1
	})
}

func TestGracefulShutdownDrainsQueuedData(t *testing.T) {
	p, err := New(detConfig(t, rule.Snapshot{}, func(c *Config) {
		c.IngestWorker = 1
	}))
	if err != nil {
		t.Fatal(err)
	}
	p.Start()
	for i := 0; i < 50; i++ {
		if err := p.Submit(model.Sample{Name: "m", Value: float64(i),
			Timestamp: time.Unix(0, int64(i))}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Stop(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if st := p.Stats(); st.Accepted != 50 {
		t.Fatalf("accepted=%d, want 50 (no half loss)", st.Accepted)
	}
}
