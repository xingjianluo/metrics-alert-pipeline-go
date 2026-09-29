// Package aggregator ingests labelled samples and produces windowed aggregates.
// It is safe for concurrent writes, tolerates out-of-order and duplicate
// samples, and rejects writes once the active-series cardinality limit is
// reached instead of dropping them silently.
package aggregator

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// RejectReason explains why a write was rejected.
type RejectReason string

const (
	// RejectCardinality means a new series exceeded the active-series limit.
	RejectCardinality RejectReason = "cardinality_limit_exceeded"
	// RejectLate means the sample belongs to a window already emitted.
	RejectLate RejectReason = "sample_behind_watermark"
	// RejectClosed means the aggregator was shut down.
	RejectClosed RejectReason = "aggregator_closed"
)

// RejectError is returned for an explicitly rejected sample.
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	return string(e.Reason) + ": " + e.Detail
}

// Config configures the aggregator.
type Config struct {
	// Window is the tumbling window length.
	Window time.Duration
	// MaxSeries bounds the number of simultaneously active series.
	MaxSeries int
	// WatermarkLag is how long a finished window waits before being emitted,
	// tolerating late-arriving samples. Zero defaults to one Window.
	WatermarkLag time.Duration
	// IdleTTL removes series unseen for this long when reclaiming cardinality.
	// Zero defaults to 10 windows.
	IdleTTL time.Duration
}

type bucket struct {
	start   time.Time
	count   int64
	sum     float64
	min     float64
	max     float64
	lastTS  time.Time
	lastVal float64
	seenTS  map[int64]float64 // unix-nano timestamp -> first value, for dedup
	dups    int64             // duplicate reports (same series+timestamp)
}

type seriesState struct {
	name     string
	labels   map[string]string
	lastSeen time.Time
	// emittedUpTo is the end (exclusive) of the newest window emitted for
	// this series. Samples before it can no longer affect any output.
	emittedUpTo time.Time
}

// Aggregator buffers samples and emits per-window aggregates.
type Aggregator struct {
	cfg Config

	mu      sync.Mutex
	closed  bool
	series  map[string]*seriesState
	buckets map[string]map[time.Time]*bucket // seriesKey -> windowStart -> bucket
	out     chan model.Aggregate

	// LateSamples counts samples that arrived after their window was emitted.
	LateSamples int64
}

// New creates an Aggregator emitting aggregates on the channel returned by Run.
func New(cfg Config) *Aggregator {
	if cfg.Window <= 0 {
		cfg.Window = time.Second
	}
	if cfg.MaxSeries <= 0 {
		cfg.MaxSeries = 1000
	}
	if cfg.WatermarkLag <= 0 {
		cfg.WatermarkLag = cfg.Window
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = 10 * cfg.Window
	}
	return &Aggregator{
		cfg:     cfg,
		series:  make(map[string]*seriesState),
		buckets: make(map[string]map[time.Time]*bucket),
		out:     make(chan model.Aggregate, 256),
	}
}

func windowStart(t time.Time, w time.Duration) time.Time {
	return t.Truncate(w)
}

// Ingest accepts one sample. It is safe for concurrent use. Samples sharing a
// series key and timestamp are deduplicated; out-of-order timestamps update
// min/max/last correctly; brand-new series beyond MaxSeries are rejected.
func (a *Aggregator) Ingest(s model.Sample) error {
	if s.Name == "" {
		return &RejectError{Reason: RejectClosed, Detail: "empty metric name"}
	}
	key := model.Fingerprint(s.Name, s.Labels)
	wStart := windowStart(s.Timestamp, a.cfg.Window)
	tsNano := s.Timestamp.UnixNano()

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return &RejectError{Reason: RejectClosed, Detail: key}
	}

	st, ok := a.series[key]
	if !ok {
		if len(a.series) >= a.cfg.MaxSeries {
			return &RejectError{
				Reason: RejectCardinality,
				Detail: "active series limit " + itoa(a.cfg.MaxSeries) +
					" reached, rejected new series " + key,
			}
		}
		st = &seriesState{name: s.Name, labels: model.CloneLabels(s.Labels)}
		a.series[key] = st
	}
	st.lastSeen = s.Timestamp

	// A window at or before the last emitted boundary can no longer change
	// any output: report the late sample rather than counting it wrongly.
	if !st.emittedUpTo.IsZero() && wStart.Add(a.cfg.Window).Compare(st.emittedUpTo) <= 0 {
		a.LateSamples++
		return &RejectError{
			Reason: RejectLate,
			Detail: key + " window " + wStart.Format(time.RFC3339Nano) + " already emitted",
		}
	}

	windows := a.buckets[key]
	if windows == nil {
		windows = make(map[time.Time]*bucket)
		a.buckets[key] = windows
	}
	b := windows[wStart]
	if b == nil {
		b = &bucket{start: wStart, seenTS: make(map[int64]float64)}
		windows[wStart] = b
	}

	if firstVal, dup := b.seenTS[tsNano]; dup {
		b.dups++
		// A conflicting value on the same timestamp is still a duplicate
		// report; the first value wins so statistics stay deterministic.
		_ = firstVal
		return nil
	}
	b.seenTS[tsNano] = s.Value

	b.count++
	b.sum += s.Value
	if b.count == 1 {
		b.min, b.max = s.Value, s.Value
	} else {
		if s.Value < b.min {
			b.min = s.Value
		}
		if s.Value > b.max {
			b.max = s.Value
		}
	}
	if s.Timestamp.After(b.lastTS) {
		b.lastTS = s.Timestamp
		b.lastVal = s.Value
	}
	return nil
}

// Run emits closed windows as the watermark advances until ctx is cancelled.
// Emit ordering is by window start, then series key.
func (a *Aggregator) Run(ctx context.Context) <-chan model.Aggregate {
	go func() {
		ticker := time.NewTicker(a.cfg.Window / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				a.advance(now)
			}
		}
	}()
	return a.out
}

func (a *Aggregator) advance(now time.Time) {
	// Emit windows ending strictly before (now - WatermarkLag).
	watermark := windowStart(now.Add(-a.cfg.WatermarkLag), a.cfg.Window)

	a.mu.Lock()
	type item struct {
		key   string
		st    *seriesState
		start time.Time
		b     *bucket
	}
	var items []item
	for key, windows := range a.buckets {
		for start, b := range windows {
			if start.Add(a.cfg.Window).Compare(watermark) <= 0 {
				items = append(items, item{key: key, st: a.series[key], start: start, b: b})
				// Detach immediately so a later tick cannot double-emit.
				// New samples for this window will be rejected as late.
				delete(windows, start)
				end := start.Add(a.cfg.Window)
				if st := a.series[key]; st != nil && end.After(st.emittedUpTo) {
					st.emittedUpTo = end
				}
			}
		}
		if len(windows) == 0 {
			delete(a.buckets, key)
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].start.Equal(items[j].start) {
			return items[i].start.Before(items[j].start)
		}
		return items[i].key < items[j].key
	})
	a.mu.Unlock()

	// Channel sends happen outside the lock. Buckets are already detached.
	for _, it := range items {
		a.out <- model.Aggregate{
			Name:        it.st.name,
			Labels:      model.CloneLabels(it.st.labels),
			WindowStart: it.start,
			WindowEnd:   it.start.Add(a.cfg.Window),
			Count:       it.b.count,
			Sum:         it.b.sum,
			Min:         it.b.min,
			Max:         it.b.max,
			Last:        it.b.lastVal,
		}
	}

	a.reclaimIdle(now)
}

func (a *Aggregator) reclaimIdle(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for key, st := range a.series {
		// Only reclaim series with no buffered buckets.
		if len(a.buckets[key]) == 0 && now.Sub(st.lastSeen) > a.cfg.IdleTTL {
			delete(a.series, key)
		}
	}
}

// Flush emits every buffered window in deterministic order, oldest first.
// It must be called after Run has stopped; it then closes the output channel.
func (a *Aggregator) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true

	type item struct {
		key   string
		st    *seriesState
		start time.Time
		b     *bucket
	}
	var items []item
	for key, windows := range a.buckets {
		for start, b := range windows {
			items = append(items, item{key: key, st: a.series[key], start: start, b: b})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].start.Equal(items[j].start) {
			return items[i].start.Before(items[j].start)
		}
		return items[i].key < items[j].key
	})
	a.mu.Unlock()

	for _, it := range items {
		a.out <- model.Aggregate{
			Name:        it.st.name,
			Labels:      model.CloneLabels(it.st.labels),
			WindowStart: it.start,
			WindowEnd:   it.start.Add(a.cfg.Window),
			Count:       it.b.count,
			Sum:         it.b.sum,
			Min:         it.b.min,
			Max:         it.b.max,
			Last:        it.b.lastVal,
		}
	}
	close(a.out)
	return nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
