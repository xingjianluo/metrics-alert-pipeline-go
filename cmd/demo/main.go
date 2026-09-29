// Command demo runs the metrics ingestion and alerting pipeline
// end to end on the local machine with a deterministic script, so every
// run shows the same explainable story:
//
//  1. concurrent writers send samples for several labeled hosts;
//  2. cpu crosses 0.8 for 2s -> Firing (one event per host);
//  3. threshold hot-updates at a point that does not wipe in-flight
//     state (it happens before the first firing);
//  4. one host flaps good/bad inside its firing episode -> no duplicate
//     Firing event;
//  5. cpu stays good for 2s -> Resolved;
//  6. a cardinality flood hits the max-series cap and gets explicit
//     cardinality_limit_exceeded rejections;
//  7. shutdown drains every queued sample (no half loss).
//
// A synthetic clock drives flush timing for determinism, while sample
// ingestion still happens through the bounded queue and real worker
// goroutines.
//
// Usage:
//
//	go run ./cmd/demo
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/pipeline"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/store"
)

const (
	window       = time.Second
	numHosts     = 3
	staleWindows = 1
	maxSeriesCap = 5 // 3 hosts + headroom; flood trips this
)

// script[host][second] = value. ForDuration 2s means three consecutive
// hot windows are needed (windowEnd run start + 2s). Host-0 flaps;
// hosts 1,2 are plain.
func valueAt(host, sec int64) float64 {
	if host == 0 {
		// 0,1 cold | 2,3,4 hot (fires at end of window 4) |
		// 5 cold (resolving) | 6 hot (flap back, NO new event) |
		// 7,8,9 cold (resolves at end of window 9)
		switch sec {
		case 2, 3, 4, 6:
			return 0.95
		default:
			return 0.2
		}
	}
	// other hosts: hot seconds 2..5, cold before and after
	if sec >= 2 && sec <= 5 {
		return 0.95
	}
	return 0.2
}

func main() {
	fast := flag.Bool("fast", false, "skip inter-step sleeps")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(log)

	rules := rule.Snapshot{Version: 1, Rules: []rule.Rule{{
		ID: "high_cpu", Metric: "cpu_usage", Field: rule.FieldAvg, Op: rule.OpGT,
		Threshold:       0.8,
		ForDuration:     2 * time.Second,
		KeepResolvedFor: 2 * time.Second,
	}}}

	p, err := pipeline.New(pipeline.Config{
		Store: store.Options{
			WindowSize: window, MaxSeries: maxSeriesCap, StaleWindows: staleWindows,
		},
		QueueSize:     64,
		Policy:        pipeline.PolicyDropNewest,
		IngestWorker:  4,
		FlushInterval: time.Hour, // driven deterministically below
		Ruleset:       rules,
		Logger:        log,
		EventSink: func(ev model.AlertEvent) {
			log.Warn("ALERT EVENT",
				"rule", ev.RuleID, "series", string(ev.Series),
				"state", ev.State.String(), "value", ev.Value,
				"fired_at", fmtTime(ev.FiredAt), "resolved_at", fmtTime(ev.ResolvedAt))
		},
	})
	if err != nil {
		log.Error("pipeline init failed", "err", err)
		os.Exit(1)
	}
	p.Start()
	log.Info("pipeline started", "max_series", maxSeriesCap, "queue", 64,
		"policy", "drop_newest", "window", window.String(), "stale_windows", staleWindows)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var totalSubmitted int64

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// pause optionally slows the script so humans can watch.
	pause := func() {
		if !*fast {
			time.Sleep(60 * time.Millisecond)
		}
	}

	// Seconds 0..9: concurrent collectors, one goroutine per host.
	for sec := int64(0); sec <= 9; sec++ {
		// Hot update at second 1 (while host states are pending): same
		// rule ID, lower threshold. Pending timers must survive.
		if sec == 1 {
			r := rules.Rules[0]
			r.Threshold = 0.5
			if err := p.UpdateRules(rule.Snapshot{Version: 2, Rules: []rule.Rule{r}}); err != nil {
				log.Error("hot update failed", "err", err)
				os.Exit(1)
			}
			log.Warn("rules hot-updated", "version", 2, "threshold", 0.5)
		}

		var wg sync.WaitGroup
		var submitted int64
		var subMu sync.Mutex
		for h := int64(0); h < numHosts; h++ {
			wg.Add(1)
			go func(h int64) {
				defer wg.Done()
				// Two samples per window; duplicates of the same
				// timestamp are merged (last-write-wins).
				for _, ms := range []int{100, 200} {
					ts := t0.Add(time.Duration(sec)*time.Second +
						time.Duration(ms)*time.Millisecond)
					if err := p.Submit(model.Sample{
						Name:      "cpu_usage",
						Labels:    map[string]string{"host": fmt.Sprintf("host-%d", h)},
						Value:     valueAt(h, sec),
						Timestamp: ts,
					}); err != nil {
						log.Warn("submit rejected", "err", err.Error())
					} else {
						subMu.Lock()
						submitted++
						subMu.Unlock()
					}
				}
			}(h)
		}
		wg.Wait()
		totalSubmitted += submitted
		// Flush window (sec-1) after its readyAt:
		// readyAt = windowEnd + staleWindows*window = (sec-1)+1+1 = sec+1.
		flushAt := t0.Add(time.Duration(sec+1) * time.Second)
		waitProcessed(ctx, log, p, totalSubmitted)
		p.FlushNow(flushAt)
		pause()
	}

	// Cardinality flood: distinct series on a separate metric exceed
	// the max-series cap but do not match the cpu rule.
	log.Info("starting cardinality flood ...")
	floodRejected := 0
	for i := 0; i < 20; i++ {
		err := p.Submit(model.Sample{
			Name:      "flood_metric",
			Labels:    map[string]string{"pod": fmt.Sprintf("pod-%d", i)},
			Value:     0.99,
			Timestamp: t0.Add(10*time.Second + time.Duration(i)*10*time.Millisecond),
		})
		if err != nil {
			floodRejected++
			continue
		}
	}
	// Drain and flush the flood window so the store-side cap is hit too.
	totalSubmitted += int64(20 - floodRejected)
	waitProcessed(ctx, log, p, totalSubmitted)
	p.FlushNow(t0.Add(12 * time.Second))

	// Print final engine states for explainability.
	for _, v := range p.Engine().Snapshot() {
		log.Info("alert state", "rule", v.RuleID, "series", string(v.Series),
			"state", v.State.String())
	}

	log.Info("shutting down gracefully ...")
	shutCtx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	if err := p.Stop(shutCtx); err != nil {
		log.Error("shutdown error", "err", err)
		os.Exit(1)
	}

	st := p.Stats()
	log.Info("final stats",
		"accepted", st.Accepted, "rejected", st.Rejected,
		"rejected_by", rejectString(st.RejectedBy),
		"queue_dropped_oldest", st.QueueDropped,
		"firing_events", st.FiringEmitted, "resolved_events", st.ResolvedEmitted,
		"flood_submit_rejected", floodRejected,
		"eval_ticks", st.EvalTicks)

	// Deterministic expectations the script is built around.
	if st.FiringEmitted != 3 {
		log.Error("UNEXPECTED firing event count", "want", 3, "got", st.FiringEmitted)
		os.Exit(2)
	}
	if st.ResolvedEmitted != 3 {
		log.Error("UNEXPECTED resolved event count", "want", 3, "got", st.ResolvedEmitted)
		os.Exit(2)
	}
	if st.RejectedBy[model.RejectCardinality] == 0 {
		log.Error("UNEXPECTED no cardinality rejection observed")
		os.Exit(2)
	}
	log.Info("demo assertions passed")
}

func waitProcessed(ctx context.Context, log *slog.Logger, p *pipeline.Pipeline, want int64) {
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := p.Stats()
		if st.Accepted+st.Rejected >= want {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond):
		}
	}
	log.Error("timeout waiting for samples to be processed", "want", want,
		"stats", p.Stats())
	os.Exit(2)
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func rejectString(m map[model.RejectReason]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k.String()] = v
	}
	return out
}
