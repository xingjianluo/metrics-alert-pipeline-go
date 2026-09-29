// Package pipeline wires ingestion, aggregation and evaluation together with
// bounded queues, configurable backpressure and graceful shutdown that drains
// all queued samples and buffered windows before stopping.
package pipeline

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/aggregator"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/evaluator"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// DropReason classifies why an item was dropped under backpressure.
type DropReason string

const (
	// DropQueueFull: ingest queue full and policy is non-blocking.
	DropQueueFull DropReason = "queue_full"
	// DropShutting: submitted after shutdown began.
	DropShutting DropReason = "shutting_down"
)

// DropEvent records one dropped item for observability.
type DropEvent struct {
	Reason DropReason
	Stage  string
	Metric string
	At     time.Time
}

// RejectEvent surfaces a sample explicitly rejected by the aggregator, with
// the business reason (cardinality / late window), distinct from backpressure.
type RejectEvent struct {
	Reason aggregator.RejectReason
	Detail string
}

// BackpressurePolicy selects queue-full behaviour.
type BackpressurePolicy int

const (
	// PolicyBlock applies backpressure: Submit blocks until space is available.
	PolicyBlock BackpressurePolicy = iota
	// PolicyDropOldest evicts the oldest queued sample to make room.
	PolicyDropOldest
	// PolicyDropNewest rejects the incoming sample.
	PolicyDropNewest
)

// Config configures the pipeline.
type Config struct {
	IngestQueueSize int
	Backpressure    BackpressurePolicy
	EvaluatorTick   time.Duration
	// LatencyBudget is the target upper bound for evaluating one aggregate.
	// Overruns are counted in Stats and logged, never silently ignored.
	LatencyBudget time.Duration
}

// Stats is a point-in-time view of pipeline counters.
type Stats struct {
	Submitted       int64
	Aggregated      int64 // aggregates produced by the aggregator
	EventsEmitted   int64
	DroppedFull     int64
	DroppedShutdown int64
	Rejected        int64 // cardinality / late-window rejections
	EvalMaxMicros   int64
	BudgetOverruns  int64
}

// ErrShuttingDown is returned by Submit once shutdown has started.
var ErrShuttingDown = errors.New("pipeline is shutting down")

// Pipeline is the runnable ingestion-to-alert pipeline.
type Pipeline struct {
	cfg  Config
	agg  *aggregator.Aggregator
	eval *evaluator.Evaluator
	log  *log.Logger

	in      chan model.Sample
	events  chan model.Event
	drops   chan DropEvent
	rejects chan RejectEvent

	mu      sync.RWMutex
	closed  bool
	started bool

	ingestDone chan struct{}
	evalDone   chan struct{}
	aggCancel  context.CancelFunc

	st submittedStats
}

// submittedStats holds the atomic counters of a Pipeline.
type submittedStats struct {
	submitted       atomic.Int64
	aggregated      atomic.Int64
	eventsEmitted   atomic.Int64
	droppedFull     atomic.Int64
	droppedShutdown atomic.Int64
	rejected        atomic.Int64
	evalMaxMicros   atomic.Int64
	budgetOverruns  atomic.Int64
}

// New constructs a pipeline.
func New(cfg Config, aggCfg aggregator.Config, rules []model.Rule) *Pipeline {
	if cfg.IngestQueueSize <= 0 {
		cfg.IngestQueueSize = 1024
	}
	if cfg.EvaluatorTick <= 0 {
		cfg.EvaluatorTick = 100 * time.Millisecond
	}
	p := &Pipeline{
		cfg:        cfg,
		agg:        aggregator.New(aggCfg),
		eval:       evaluator.New(rules),
		log:        log.Default(),
		in:         make(chan model.Sample, cfg.IngestQueueSize),
		events:     make(chan model.Event, 1024),
		drops:      make(chan DropEvent, 4096),
		rejects:    make(chan RejectEvent, 4096),
		ingestDone: make(chan struct{}),
		evalDone:   make(chan struct{}),
	}
	return p
}

// SetLogger replaces the logger used for pipeline-level messages.
func (p *Pipeline) SetLogger(l *log.Logger) {
	p.log = l
	p.eval.SetLogger(l)
}

// Start launches the aggregation and evaluation workers.
func (p *Pipeline) Start(ctx context.Context) {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return
	}
	p.started = true
	aggCtx, cancel := context.WithCancel(ctx)
	p.aggCancel = cancel
	p.mu.Unlock()

	aggOut := p.agg.Run(aggCtx)

	// Worker 1: drain the ingest queue into the aggregator.
	go func() {
		defer close(p.ingestDone)
		for s := range p.in {
			if err := p.agg.Ingest(s); err != nil {
				var rerr *aggregator.RejectError
				if errors.As(err, &rerr) {
					p.st.rejected.Add(1)
					p.emitReject(RejectEvent{Reason: rerr.Reason, Detail: rerr.Detail})
					continue
				}
				p.log.Printf("[pipeline] unexpected ingest error: %v", err)
			}
		}
	}()

	// Worker 2: turn aggregates into alert events.
	go func() {
		defer close(p.evalDone)
		ticker := time.NewTicker(p.cfg.EvaluatorTick)
		defer ticker.Stop()
		for {
			select {
			case a, ok := <-aggOut:
				if !ok {
					return // aggregator flushed and closed during shutdown
				}
				p.st.aggregated.Add(1)
				begin := time.Now()
				evs := p.eval.Evaluate(a)
				p.recordEvalLatency(time.Since(begin))
				for _, ev := range evs {
					p.events <- ev
					p.st.eventsEmitted.Add(1)
				}
			case now := <-ticker.C:
				for _, ev := range p.eval.Tick(now) {
					p.events <- ev
					p.st.eventsEmitted.Add(1)
				}
			}
		}
	}()
}

// Submit enqueues one sample. Behaviour when the queue is full depends on the
// configured Backpressure policy. It never silently drops: non-blocking drops
// are reported on the Drops channel.
func (p *Pipeline) Submit(s model.Sample) error {
	// The read lock is held for the whole enqueue so Shutdown cannot close
	// the queue while a send is in flight (send-on-closed would panic).
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		// Observation channels are already closed during shutdown; just count.
		p.st.droppedShutdown.Add(1)
		return ErrShuttingDown
	}

	switch p.cfg.Backpressure {
	case PolicyBlock:
		p.in <- s
		p.st.submitted.Add(1)
		return nil
	case PolicyDropNewest:
		select {
		case p.in <- s:
			p.st.submitted.Add(1)
			return nil
		default:
			p.emitDrop(DropEvent{Reason: DropQueueFull, Stage: "ingest", Metric: s.Name, At: time.Now()})
			return nil
		}
	default: // PolicyDropOldest
		for {
			select {
			case p.in <- s:
				p.st.submitted.Add(1)
				return nil
			default:
				select {
				case _, ok := <-p.in:
					if !ok {
						return ErrShuttingDown
					}
					p.emitDrop(DropEvent{Reason: DropQueueFull, Stage: "ingest", Metric: s.Name, At: time.Now()})
				default:
				}
			}
		}
	}
}

// Events returns the channel of deduplicated alert transition events.
func (p *Pipeline) Events() <-chan model.Event { return p.events }

// Drops returns the channel observing backpressure drops.
func (p *Pipeline) Drops() <-chan DropEvent { return p.drops }

// Rejects returns the channel observing aggregator rejections.
func (p *Pipeline) Rejects() <-chan RejectEvent { return p.rejects }

// UpdateRules hot-reloads alerting rules without touching existing state.
func (p *Pipeline) UpdateRules(rules []model.Rule) {
	p.eval.UpdateRules(rules)
}

// Snapshot exposes the current alert states (for tests/introspection).
func (p *Pipeline) Snapshot() map[string]evaluator.AlertStatus {
	return p.eval.Snapshot()
}

// Stats returns a point-in-time copy of the pipeline counters.
func (p *Pipeline) Stats() Stats {
	return Stats{
		Submitted:       p.st.submitted.Load(),
		Aggregated:      p.st.aggregated.Load(),
		EventsEmitted:   p.st.eventsEmitted.Load(),
		DroppedFull:     p.st.droppedFull.Load(),
		DroppedShutdown: p.st.droppedShutdown.Load(),
		Rejected:        p.st.rejected.Load(),
		EvalMaxMicros:   p.st.evalMaxMicros.Load(),
		BudgetOverruns:  p.st.budgetOverruns.Load(),
	}
}

func (p *Pipeline) recordEvalLatency(d time.Duration) {
	micros := d.Microseconds()
	for {
		cur := p.st.evalMaxMicros.Load()
		if micros <= cur || p.st.evalMaxMicros.CompareAndSwap(cur, micros) {
			break
		}
	}
	if p.cfg.LatencyBudget > 0 && d > p.cfg.LatencyBudget {
		n := p.st.budgetOverruns.Add(1)
		p.log.Printf("[pipeline] evaluation latency %v exceeded budget %v (overrun #%d)",
			d, p.cfg.LatencyBudget, n)
	}
}

// Shutdown performs an ordered drain so queued data is not cut in half:
//  1. reject new submissions,
//  2. drain the ingest queue (all accepted samples are aggregated),
//  3. flush every buffered window out of the aggregator,
//  4. let the evaluator consume all aggregates, then close the events channel.
//
// It waits at most until ctx expires.
func (p *Pipeline) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	started := p.started
	close(p.in) // safe: every Submit holds RLock while sending
	p.mu.Unlock()

	// If Start was never called there are no workers to wait for.
	if !started {
		close(p.drops)
		close(p.rejects)
		close(p.events)
		return nil
	}

	// Step 2: wait for the ingest worker to finish the whole queue.
	select {
	case <-p.ingestDone:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Step 3: stop the watermark ticker, then flush all buffered windows.
	p.aggCancel()
	if err := p.agg.Close(); err != nil {
		return err
	}

	// Step 4: evaluator returns once the aggregator output channel is closed.
	select {
	case <-p.evalDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	// All workers are stopped: no further drops/rejects can be emitted.
	close(p.drops)
	close(p.rejects)
	close(p.events)
	return nil
}

func (p *Pipeline) emitDrop(d DropEvent) {
	switch d.Reason {
	case DropQueueFull:
		p.st.droppedFull.Add(1)
	case DropShutting:
		p.st.droppedShutdown.Add(1)
	}
	select {
	case p.drops <- d:
	default:
	}
}

func (p *Pipeline) emitReject(r RejectEvent) {
	select {
	case p.rejects <- r:
	default:
	}
}
