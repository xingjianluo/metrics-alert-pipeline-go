// Package pipeline wires ingestion workers, the evaluator ticker and a
// bounded queue, providing backpressure, explicit overflow policies and
// graceful shutdown.
//
// Latency budget: the store only flushes a window once it can no longer
// receive an in-policy late sample (windowEnd + StaleWindows*WindowSize).
// An alert whose last sustaining window lands at T is therefore produced
// at most FlushInterval after T + StaleWindows*WindowSize, deterministically.
//
// Graceful shutdown: Stop closes the queue (new writes rejected), waits
// for workers to ingest every queued sample, then runs one final flush
// and evaluation so queued data is never lost in half.
package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/checkpoint"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/engine"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/store"
)

// Config configures a Pipeline.
type Config struct {
	Store        store.Options
	QueueSize    int
	Policy       OverflowPolicy
	IngestWorker int
	// FlushInterval is how often the store flushes ready windows.
	FlushInterval time.Duration
	// Ruleset is the initial rule snapshot.
	Ruleset rule.Snapshot
	// EventSink receives Firing/Resolved events (may be nil).
	EventSink func(model.AlertEvent)
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Clock is overridable in tests.
	Clock func() time.Time

	// CheckpointDir enables state persistence when non-empty. Alert
	// states, unflushed windows and recent event history are saved
	// periodically and on Stop, and loaded back on New.
	CheckpointDir string
	// CheckpointInterval is how often state is saved (default 10s).
	// Saving runs on its own goroutine and never blocks ingestion or
	// evaluation beyond a short state copy.
	CheckpointInterval time.Duration
	// MaxCheckpoints bounds retained checkpoint files (default 3).
	MaxCheckpoints int
	// MaxEvents bounds the persisted event history (default 256).
	MaxEvents int
}

// Stats is a point-in-time operational snapshot.
type Stats struct {
	QueueLen        int
	Series          int
	Accepted        int64
	Rejected        int64
	RejectedBy      map[model.RejectReason]int64
	QueueDropped    int64 // DropOldest evictions
	FiringEmitted   int64
	ResolvedEmitted int64
	EvalTicks       int64
}

// Pipeline runs the local collection -> alerting flow.
type Pipeline struct {
	cfg    Config
	log    *slog.Logger
	store  *store.Store
	engine *engine.Engine
	queue  *boundedQueue
	now    func() time.Time

	wg      sync.WaitGroup
	stopCh  chan struct{}
	stopped chan struct{}

	mu         sync.Mutex
	accepted   int64
	rejected   int64
	rejectedBy map[model.RejectReason]int64
	firing     int64
	resolved   int64
	ticks      int64

	ckpt     *checkpoint.Manager // nil when persistence disabled
	ckptMu   sync.Mutex          // serializes Save (ticker vs Stop)
	events   []model.AlertEvent  // bounded recent-event history
	maxEvent int
}

// New constructs a Pipeline.
func New(cfg Config) (*Pipeline, error) {
	if err := cfg.Ruleset.Validate(); err != nil {
		return nil, err
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 1024
	}
	if cfg.IngestWorker <= 0 {
		cfg.IngestWorker = 2
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	now := cfg.Clock
	if now == nil {
		now = time.Now
	}
	st := store.New(&cfg.Store)
	p := &Pipeline{
		cfg: cfg, log: log, store: st,
		engine: engine.New(cfg.Ruleset),
		queue:  newBoundedQueue(cfg.QueueSize, cfg.Policy),
		now:    now, stopCh: make(chan struct{}),
		rejectedBy: map[model.RejectReason]int64{},
		maxEvent:   cfg.MaxEvents,
	}
	if p.maxEvent <= 0 {
		p.maxEvent = 256
	}
	if cfg.CheckpointDir != "" {
		mgr, err := checkpoint.NewManager(checkpoint.Options{
			Dir:            cfg.CheckpointDir,
			MaxCheckpoints: cfg.MaxCheckpoints,
			MaxEvents:      cfg.MaxEvents,
		})
		if err != nil {
			return nil, err
		}
		p.ckpt = mgr
		if err := p.restore(); err != nil {
			// Hard failure (corrupt/incompatible checkpoint): refuse to
			// start rather than silently run on empty state.
			return nil, err
		}
	}
	return p, nil
}

// restore loads the newest usable checkpoint and applies it with the
// same compatibility rule as a hot update: states of rule IDs still
// present continue, states of removed rules are retired (logged), new
// rules start clean.
func (p *Pipeline) restore() error {
	payload, skips, err := p.ckpt.LoadWithLog()
	if err != nil {
		if errors.Is(err, checkpoint.ErrNoCheckpoint) {
			p.log.Info("no checkpoint found, starting clean", "dir", p.cfg.CheckpointDir)
			return nil
		}
		return err
	}
	for _, s := range skips {
		p.log.Warn("checkpoint skipped", "reason", s)
	}
	dropped := p.engine.Restore(payload.Alerts)
	for _, d := range dropped {
		p.log.Info("checkpoint state retired (rule not in current ruleset)",
			"rule", d.RuleID, "series", string(d.Series), "state", d.State.String())
	}
	if skipped := p.store.Restore(payload.Windows); skipped > 0 {
		p.log.Warn("checkpoint series skipped at cardinality cap", "count", skipped)
	}
	if len(payload.Events) > p.maxEvent {
		payload.Events = payload.Events[len(payload.Events)-p.maxEvent:]
	}
	p.events = append(p.events, payload.Events...)
	p.log.Info("checkpoint restored",
		"written_at", payload.WrittenAt.Format(time.RFC3339),
		"rule_version", payload.RuleVersion,
		"alerts", len(payload.Alerts), "series", len(payload.Windows.Series),
		"events", len(payload.Events))
	return nil
}

// saveCheckpoint snapshots state and writes one checkpoint. It is
// serialized between the periodic ticker and the shutdown save.
func (p *Pipeline) saveCheckpoint() {
	if p.ckpt == nil {
		return
	}
	p.ckptMu.Lock()
	defer p.ckptMu.Unlock()
	snap := p.engine.Rules()
	p.mu.Lock()
	events := make([]model.AlertEvent, len(p.events))
	copy(events, p.events)
	p.mu.Unlock()
	alerts := p.engine.Export()
	err := p.ckpt.Save(checkpoint.Payload{
		WrittenAt:   p.now(),
		RuleVersion: snap.Version,
		Rules:       snap.Rules,
		Alerts:      alerts,
		Windows:     p.store.Export(),
		Events:      events,
	})
	if err != nil {
		p.log.Error("checkpoint save failed", "err", err)
		return
	}
	p.log.Info("checkpoint saved", "dir", p.cfg.CheckpointDir,
		"alerts", len(alerts), "series", p.store.SeriesCount())
}

// SaveCheckpointNow forces one checkpoint immediately. Tests and the
// deterministic demo use it; the periodic loop and Stop call it too.
func (p *Pipeline) SaveCheckpointNow() { p.saveCheckpoint() }

// EventHistory returns the bounded recent-event history (oldest first).
func (p *Pipeline) EventHistory() []model.AlertEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]model.AlertEvent, len(p.events))
	copy(out, p.events)
	return out
}

// Start launches ingestion workers and the evaluator loop.
func (p *Pipeline) Start() {
	for i := 0; i < p.cfg.IngestWorker; i++ {
		p.wg.Add(1)
		go p.ingestWorker(i)
	}
	p.wg.Add(1)
	go p.evalLoop()
	if p.ckpt != nil {
		p.wg.Add(1)
		go p.checkpointLoop()
	}
}

// checkpointLoop saves state on its own ticker so ingestion and
// evaluation are never blocked by disk I/O beyond the short state copy.
func (p *Pipeline) checkpointLoop() {
	defer p.wg.Done()
	iv := p.cfg.CheckpointInterval
	if iv <= 0 {
		iv = 10 * time.Second
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-t.C:
			p.saveCheckpoint()
		}
	}
}

// Submit enqueues one sample. It blocks or rejects according to the
// overflow policy. Rejections carry an explicit reason.
func (p *Pipeline) Submit(s model.Sample) error {
	if err := p.queue.Push(s); err != nil {
		p.countReject(err)
		return err
	}
	return nil
}

// SubmitBatch enqueues multiple samples, reporting the first failure
// (earlier samples already accepted stay queued).
func (p *Pipeline) SubmitBatch(ss []model.Sample) error {
	for _, s := range ss {
		if err := p.Submit(s); err != nil {
			return err
		}
	}
	return nil
}

// UpdateRules hot-swaps the ruleset, preserving in-flight alert state.
func (p *Pipeline) UpdateRules(snap rule.Snapshot) error {
	return p.engine.UpdateRules(snap)
}

func (p *Pipeline) ingestWorker(id int) {
	defer p.wg.Done()
	for {
		s, ok := p.queue.Pop()
		if !ok {
			return
		}
		if err := p.store.Ingest(s); err != nil {
			p.countReject(err)
			p.log.Warn("sample rejected",
				"worker", id, "metric", s.Name, "labels", s.Labels,
				"ts", s.Timestamp.Format(time.RFC3339Nano), "err", err.Error())
			continue
		}
		p.mu.Lock()
		p.accepted++
		p.mu.Unlock()
	}
}

func (p *Pipeline) evalLoop() {
	defer p.wg.Done()
	t := time.NewTicker(p.cfg.FlushInterval)
	defer t.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case now := <-t.C:
			p.evaluateOnce(now)
		}
	}
}

// evaluateOnce flushes ready windows and runs the engine. It is also
// invoked a final time during graceful shutdown.
func (p *Pipeline) evaluateOnce(now time.Time) {
	aggs := p.store.Flush(now)
	if len(aggs) == 0 {
		p.mu.Lock()
		p.ticks++
		p.mu.Unlock()
		return
	}
	// Log the exact inputs of the decision.
	for _, a := range aggs {
		p.log.Info("eval input",
			"series", string(a.Series), "metric", a.Name, "labels", a.Labels,
			"window", a.WindowStart.Format(time.RFC3339),
			"count", a.Count, "sum", a.Sum, "avg", safeAvg(a),
			"latest", a.Latest)
	}
	events := p.engine.Evaluate(aggs)
	for _, ev := range events {
		p.log.Info("eval result",
			"rule", ev.RuleID, "series", string(ev.Series),
			"state", ev.State.String(), "value", ev.Value,
			"eval_time", ev.EvalTime.Format(time.RFC3339),
			"fired_at", formatTime(ev.FiredAt), "resolved_at", formatTime(ev.ResolvedAt))
		if ev.State == model.StateFiring {
			p.mu.Lock()
			p.firing++
			p.mu.Unlock()
		} else {
			p.mu.Lock()
			p.resolved++
			p.mu.Unlock()
		}
		if p.cfg.EventSink != nil {
			p.cfg.EventSink(ev)
		}
		p.mu.Lock()
		p.events = append(p.events, ev)
		if len(p.events) > p.maxEvent {
			p.events = p.events[len(p.events)-p.maxEvent:]
		}
		p.mu.Unlock()
	}
	p.mu.Lock()
	p.ticks++
	p.mu.Unlock()
}

// Stop performs graceful shutdown: reject new writes, ingest every
// queued sample, then flush/evaluate one last time. The context bounds
// the total drain time.
func (p *Pipeline) Stop(ctx context.Context) error {
	close(p.stopCh)
	p.queue.Close()

	done := make(chan struct{})
	go func() {
		p.wg.Wait() // workers + eval loop + checkpoint loop
		// Final deterministic flush/evaluate after all queued data is in.
		p.evaluateOnce(p.now())
		// Persist the final state so a restart continues exactly here.
		p.saveCheckpoint()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stats returns operational counters.
func (p *Pipeline) Stats() Stats {
	p.mu.Lock()
	by := make(map[model.RejectReason]int64, len(p.rejectedBy))
	for k, v := range p.rejectedBy {
		by[k] = v
	}
	st := Stats{
		QueueLen: p.queue.Len(), Series: p.store.SeriesCount(),
		Accepted: p.accepted, Rejected: p.rejected, RejectedBy: by,
		QueueDropped:  p.queue.DroppedCount(),
		FiringEmitted: p.firing, ResolvedEmitted: p.resolved,
		EvalTicks: p.ticks,
	}
	p.mu.Unlock()
	return st
}

// FlushNow runs one flush + evaluation pass at the given time. It is
// used by the deterministic demo and tests; the production loop calls
// it internally on FlushInterval.
func (p *Pipeline) FlushNow(now time.Time) { p.evaluateOnce(now) }

// Store and Engine are exposed for tests and advanced embedding.
func (p *Pipeline) Store() *store.Store    { return p.store }
func (p *Pipeline) Engine() *engine.Engine { return p.engine }

func (p *Pipeline) countReject(err error) {
	reason := model.RejectInvalid
	if re, ok := err.(*model.RejectError); ok {
		reason = re.Reason
	}
	p.mu.Lock()
	p.rejected++
	p.rejectedBy[reason]++
	p.mu.Unlock()
}

func safeAvg(a model.WindowAggregate) float64 {
	if a.Count == 0 {
		return 0
	}
	return a.Sum / float64(a.Count)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
