// Command restartdemo demonstrates state persistence: it runs the
// pipeline until an alert fires, stops (which writes a final
// checkpoint), then starts a fresh pipeline over the same checkpoint
// directory and shows that
//
//   - the firing episode continues: no duplicate Firing is emitted;
//   - the resolving timer continues: exactly one Resolved is emitted;
//   - restored pending timers keep their original start time.
//
// The run self-checks and exits non-zero on any unexpected count.
//
// Usage:
//
//	go run ./cmd/restartdemo            # uses a temp checkpoint dir
//	go run ./cmd/restartdemo -keep-dir  # keep the dir and print its path
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/pipeline"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/store"
)

var rules = rule.Snapshot{Version: 1, Rules: []rule.Rule{{
	ID: "high_cpu", Metric: "cpu_usage", Field: rule.FieldAvg, Op: rule.OpGT,
	Threshold: 0.8, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second,
}}}

func main() {
	keepDir := flag.Bool("keep-dir", false, "keep the checkpoint directory and print its path")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dir, err := os.MkdirTemp("", "restartdemo-*")
	if err != nil {
		log.Error("mktemp failed", "err", err)
		os.Exit(1)
	}
	if !*keepDir {
		defer os.RemoveAll(dir)
	} else {
		log.Info("checkpoint dir kept", "dir", dir)
	}

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// ---- Phase 1: run until the alert fires, then stop. ----
	p1 := mustPipeline(log, dir)
	p1.Start()
	log.Info("phase 1: feeding hot windows until firing ...")
	var n1 int64
	for sec := int64(0); sec <= 2; sec++ {
		n1++
		submit(log, p1, 0.95, t0, sec, n1)
		flush(p1, t0, sec)
	}
	if got := p1.Stats().FiringEmitted; got != 1 {
		log.Error("phase 1 UNEXPECTED firing count", "want", 1, "got", got)
		os.Exit(2)
	}
	log.Info("phase 1: alert firing, stopping (writes final checkpoint) ...")
	stop(log, p1)
	files, _ := filepath.Glob(filepath.Join(dir, "checkpoint-*.json"))
	log.Info("phase 1: checkpoint files on disk", "files", files)

	// ---- Phase 2: restart over the same dir and continue. ----
	p2 := mustPipeline(log, dir)
	for _, v := range p2.Engine().Snapshot() {
		log.Info("phase 2: restored alert state", "rule", v.RuleID,
			"series", string(v.Series), "state", v.State.String(),
			"since", v.Since.Format(time.RFC3339),
			"fired_at", v.FiredAt.Format(time.RFC3339))
	}
	p2.Start()
	log.Info("phase 2: feeding cold windows, expecting exactly one Resolved ...")
	var n2 int64
	for sec := int64(3); sec <= 5; sec++ {
		n2++
		submit(log, p2, 0.2, t0, sec, n2)
		flush(p2, t0, sec)
	}
	stop(log, p2)

	st := p2.Stats()
	log.Info("phase 2 stats", "firing_events", st.FiringEmitted,
		"resolved_events", st.ResolvedEmitted)
	if st.FiringEmitted != 0 {
		log.Error("UNEXPECTED duplicate Firing after restart", "got", st.FiringEmitted)
		os.Exit(2)
	}
	if st.ResolvedEmitted != 1 {
		log.Error("UNEXPECTED resolved count after restart", "want", 1, "got", st.ResolvedEmitted)
		os.Exit(2)
	}
	log.Info("restartdemo assertions passed: no duplicate firing, resolve not lost")
}

func mustPipeline(log *slog.Logger, dir string) *pipeline.Pipeline {
	p, err := pipeline.New(pipeline.Config{
		Store: store.Options{
			WindowSize: time.Second, MaxSeries: 10, StaleWindows: 1,
		},
		QueueSize:     64,
		Policy:        pipeline.PolicyDropNewest,
		FlushInterval: time.Hour, // driven deterministically
		Ruleset:       rules,
		Logger:        log,
		CheckpointDir: dir,
		EventSink: func(ev model.AlertEvent) {
			log.Warn("ALERT EVENT", "rule", ev.RuleID, "series", string(ev.Series),
				"state", ev.State.String(), "value", ev.Value)
		},
	})
	if err != nil {
		log.Error("pipeline init failed", "err", err)
		os.Exit(1)
	}
	return p
}

// submit feeds one sample and waits until the pipeline has processed
// want samples in total (counters are per pipeline instance).
func submit(log *slog.Logger, p *pipeline.Pipeline, v float64, t0 time.Time, sec, want int64) {
	if err := p.Submit(model.Sample{
		Name:      "cpu_usage",
		Labels:    map[string]string{"host": "host-0"},
		Value:     v,
		Timestamp: t0.Add(time.Duration(sec) * time.Second),
	}); err != nil {
		log.Error("submit failed", "err", err)
		os.Exit(1)
	}
	// Wait for the worker to ingest before the deterministic flush.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := p.Stats()
		if st.Accepted+st.Rejected >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	log.Error("timeout waiting for ingest")
	os.Exit(2)
}

func flush(p *pipeline.Pipeline, t0 time.Time, sec int64) {
	// readyAt = windowEnd + StaleWindows*window = sec+2.
	p.FlushNow(t0.Add(time.Duration(sec+2) * time.Second))
}

func stop(log *slog.Logger, p *pipeline.Pipeline) {
	ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	if err := p.Stop(ctx); err != nil {
		log.Error("stop failed", "err", err)
		os.Exit(1)
	}
}
