package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/aggregator"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

func rule() model.Rule {
	return model.Rule{
		ID: "r-cpu", Metric: "cpu",
		MatchLabels:  map[string]string{},
		Agg:          model.AggLast,
		Op:           model.OpGT,
		Threshold:    80,
		Window:       time.Minute,
		ForDuration:  2 * time.Minute,
		KeepDuration: 2 * time.Minute,
	}
}

// TestConcurrentWritesThenGracefulShutdown proves every accepted sample is
// accounted for: concurrent writers + shutdown drain must not cut queued data.
func TestConcurrentWritesThenGracefulShutdown(t *testing.T) {
	// Huge window so nothing is watermark-emitted; all output comes from flush.
	aggCfg := aggregator.Config{Window: time.Hour, MaxSeries: 8}
	p := New(Config{
		IngestQueueSize: 16,
		Backpressure:    PolicyBlock,
		EvaluatorTick:   time.Hour, // keep ticker quiet
	}, aggCfg, []model.Rule{rule()})

	p.Start(context.Background())

	const writers = 8
	const perWriter = 100
	t0 := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// writer -> host label, each sample a distinct second so
				// concurrent duplicate reports cannot collapse counts.
				err := p.Submit(model.Sample{
					Name:      "cpu",
					Labels:    map[string]string{"host": string(rune('a' + w))},
					Value:     float64(w*perWriter + i),
					Timestamp: t0.Add(time.Duration(w*perWriter+i) * time.Second),
				})
				if err != nil {
					t.Errorf("submit: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(shCtx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	var events int
	for range p.Events() {
		events++
	}
	st := p.Stats()
	if st.Submitted != int64(writers*perWriter) {
		t.Fatalf("submitted=%d want %d", st.Submitted, writers*perWriter)
	}
	if st.Aggregated != int64(writers) {
		t.Fatalf("aggregated=%d want %d (one window per host)", st.Aggregated, writers)
	}
	if st.DroppedFull != 0 || st.Rejected != 0 {
		t.Fatalf("no drops/rejects expected under blocking policy, got %+v", st)
	}
	t.Logf("events=%d stats=%+v", events, st)
}

// TestBackpressureDropsDistinguishReason checks that overload drops are
// observable and classified distinctly from post-shutdown submissions.
func TestBackpressureDropsDistinguishReason(t *testing.T) {
	aggCfg := aggregator.Config{Window: time.Hour, MaxSeries: 10000}
	p := New(Config{
		IngestQueueSize: 2,
		Backpressure:    PolicyDropNewest,
		EvaluatorTick:   time.Hour,
	}, aggCfg, []model.Rule{rule()})
	p.Start(context.Background())

	t0 := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 5000; i++ {
		err := p.Submit(model.Sample{
			Name:      "cpu",
			Labels:    map[string]string{"host": "h"},
			Value:     1,
			Timestamp: t0.Add(time.Duration(i) * time.Millisecond),
		})
		if err != nil {
			t.Fatalf("drop policy must not error while running: %v", err)
		}
	}

	var fullDrops int
drainDrops:
	for {
		select {
		case d := <-p.Drops():
			if d.Reason == DropQueueFull {
				fullDrops++
			}
			if d.Stage != "ingest" {
				t.Fatalf("drop stage = %q, want ingest", d.Stage)
			}
		default:
			break drainDrops
		}
	}
	if fullDrops == 0 {
		t.Fatal("expected queue_full drops under overload with a tiny queue")
	}
	if got := p.Stats().DroppedFull; got == 0 {
		t.Fatal("counter DroppedFull = 0 despite observed drops")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Post-shutdown submit is a different reason, not a queue-full drop.
	err := p.Submit(model.Sample{Name: "cpu", Timestamp: t0})
	if !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("want ErrShuttingDown, got %v", err)
	}
	if p.Stats().DroppedShutdown < 1 {
		t.Fatal("DroppedShutdown counter not incremented")
	}
}

// TestCardinalityRejectedEndToEnd verifies the rejection reason reaches the
// pipeline observer and the rejected series never enters statistics.
func TestCardinalityRejectedEndToEnd(t *testing.T) {
	aggCfg := aggregator.Config{Window: time.Hour, MaxSeries: 2}
	p := New(Config{IngestQueueSize: 4, Backpressure: PolicyBlock, EvaluatorTick: time.Hour},
		aggCfg, []model.Rule{rule()})
	p.Start(context.Background())
	t0 := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

	submit := func(host string) error {
		return p.Submit(model.Sample{
			Name: "cpu", Labels: map[string]string{"host": host},
			Value: 1, Timestamp: t0,
		})
	}
	if err := submit("a"); err != nil {
		t.Fatal(err)
	}
	if err := submit("b"); err != nil {
		t.Fatal(err)
	}
	if err := submit("c"); err != nil {
		t.Fatal(err)
	}

	var rej RejectEvent
	select {
	case rej = <-p.Rejects():
	case <-time.After(2 * time.Second):
		t.Fatal("rejection event not observed")
	}
	if rej.Reason != aggregator.RejectCardinality {
		t.Fatalf("reason=%s want cardinality_limit_exceeded, detail: %s", rej.Reason, rej.Detail)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if p.Stats().Aggregated != 2 {
		t.Fatalf("want 2 admitted series, got %d aggregates", p.Stats().Aggregated)
	}
}

// writeNow submits one sample timestamped at the current wall-clock time so it
// always precedes its window watermark in the real-time tests below.
func writeNow(t *testing.T, p *Pipeline, v float64) {
	t.Helper()
	if err := p.Submit(model.Sample{
		Name: "cpu", Labels: map[string]string{"host": "h"},
		Value: v, Timestamp: time.Now(),
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
}

// TestHotUpdateDuringEvaluation verifies that replacing a rule while an alert
// is firing preserves its state and never re-fires the same episode.
func TestHotUpdateDuringEvaluation(t *testing.T) {
	w := 30 * time.Millisecond
	aggCfg := aggregator.Config{Window: w, MaxSeries: 10, WatermarkLag: 20 * time.Millisecond}
	r := model.Rule{
		ID: "r1", Metric: "cpu",
		Agg: model.AggLast, Op: model.OpGT, Threshold: 80,
		Window: w, ForDuration: 2 * w, KeepDuration: 2 * w,
	}
	p := New(Config{
		IngestQueueSize: 64, Backpressure: PolicyBlock,
		EvaluatorTick: 10 * time.Millisecond,
	}, aggCfg, []model.Rule{r})
	p.Start(context.Background())

	writeNow(t, p, 90)
	time.Sleep(2 * w)
	writeNow(t, p, 90)

	// The fired event is consumed here, so seed the counter with it.
	fired := 0
	deadline := time.After(3 * time.Second)
waiting:
	for {
		select {
		case ev := <-p.Events():
			if ev.Type == model.EventFired {
				fired++
				break waiting
			}
		case <-deadline:
			t.Fatalf("timed out waiting for fired, stats=%+v", p.Stats())
		}
	}

	key := "r1|cpu{host=h}"
	if s := p.Snapshot()[key]; s.State != model.StateFiring {
		t.Fatalf("want firing before hot update, got %s", s.State)
	}

	// Hot-update: same ID, changed threshold/timings. State is preserved.
	r.Threshold = 50
	r.ForDuration = 10 * w
	p.UpdateRules([]model.Rule{r})
	if s := p.Snapshot()[key]; s.State != model.StateFiring {
		t.Fatalf("hot update cleared firing state: %s", s.State)
	}

	// Keep violating: the same episode must not fire again.
	for i := 0; i < 3; i++ {
		writeNow(t, p, 90)
		time.Sleep(2 * w)
	}
	// Then recover long enough to resolve.
	for i := 0; i < 3; i++ {
		writeNow(t, p, 5)
		time.Sleep(2 * w)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	var resolved int
	for ev := range p.Events() {
		switch ev.Type {
		case model.EventFired:
			fired++
		case model.EventResolved:
			resolved++
		}
	}
	if fired != 1 || resolved != 1 {
		t.Fatalf("hot update must not duplicate events: fired=%d resolved=%d", fired, resolved)
	}
}

// TestEndToEndAlertLifecycle drives real windowed evaluation and asserts the
// fired/recovered sequence with no duplicate fired events.
func TestEndToEndAlertLifecycle(t *testing.T) {
	w := 30 * time.Millisecond
	aggCfg := aggregator.Config{Window: w, MaxSeries: 10, WatermarkLag: 20 * time.Millisecond}
	p := New(Config{
		IngestQueueSize: 64, Backpressure: PolicyBlock,
		EvaluatorTick: 10 * time.Millisecond, LatencyBudget: 100 * time.Millisecond,
	}, aggCfg, []model.Rule{{
		ID: "r1", Metric: "cpu",
		Agg: model.AggLast, Op: model.OpGT, Threshold: 80,
		Window: w, ForDuration: 2 * w, KeepDuration: 2 * w,
	}})
	p.Start(context.Background())

	for k := 0; k < 4; k++ {
		writeNow(t, p, 90)
		time.Sleep(2 * w)
	}
	for k := 0; k < 4; k++ {
		writeNow(t, p, 5)
		time.Sleep(2 * w)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	var seq []model.EventType
	for ev := range p.Events() {
		seq = append(seq, ev.Type)
	}
	var fired, resolved int
	for _, ty := range seq {
		if ty == model.EventFired {
			fired++
		}
		if ty == model.EventResolved {
			resolved++
		}
	}
	if fired != 1 || resolved != 1 {
		t.Fatalf("want exactly one fired + one resolved, got fired=%d resolved=%d seq=%v",
			fired, resolved, seq)
	}
	if p.Stats().BudgetOverruns != 0 {
		t.Fatalf("evaluation exceeded latency budget: %+v", p.Stats())
	}
	t.Logf("event sequence: %v", seq)
}
