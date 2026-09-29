// Package store holds recent samples per series and produces window
// aggregates. It enforces a cardinality cap and tolerates concurrent,
// out-of-order and duplicate writes.
//
// Concurrency: a single RWMutex guards all series maps. Correct and
// explainable at local scale; the pipeline layer bounds total writers.
//
// Duplicate policy: samples with the same series AND the same exact
// timestamp are considered duplicate reports. The latest write wins
// (replaces the value); count/sum/min/max are never double counted.
//
// Out-of-order policy: windows are aligned by truncating the sample
// timestamp to WindowSize. A sample whose window is more than
// StaleWindows windows behind the newest window seen so far is rejected
// with RejectStale. Late samples within the bound simply update the
// earlier window, whose aggregate is only flushed once it can no longer
// receive accepted updates (see Flush).
package store

import (
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// Options configures a Store. Zero fields fall back to defaults.
type Options struct {
	// WindowSize is the aggregation bucket width (default 1s).
	WindowSize time.Duration
	// MaxSeries is the hard cap of distinct label combinations
	// (default 10000). A new series beyond the cap is rejected.
	MaxSeries int
	// StaleWindows bounds how late a sample may arrive, measured in
	// windows behind the newest observed window (default 2).
	StaleWindows int
}

func (o *Options) withDefaults() {
	if o.WindowSize <= 0 {
		o.WindowSize = time.Second
	}
	if o.MaxSeries <= 0 {
		o.MaxSeries = 10000
	}
	if o.StaleWindows <= 0 {
		o.StaleWindows = 2
	}
}

type windowAgg struct {
	// values maps each distinct sample timestamp to its (last-written)
	// value. Map presence is the source of truth for count and makes
	// duplicate replacement exact.
	values map[time.Time]float64
}

type seriesState struct {
	name    string
	labels  map[string]string
	windows map[time.Time]*windowAgg
}

// Store ingests samples and serves window aggregates.
type Store struct {
	opt Options

	mu           sync.RWMutex
	series       map[model.SeriesKey]*seriesState
	latestWindow time.Time // newest windowStart ever observed
}

// New constructs an empty Store.
func New(opt *Options) *Store {
	var o Options
	if opt != nil {
		o = *opt
	}
	o.withDefaults()
	return &Store{
		opt:    o,
		series: make(map[model.SeriesKey]*seriesState),
	}
}

// Options returns the resolved options.
func (s *Store) Options() Options { return s.opt }

// Ingest accepts one sample. It returns *model.RejectError with an
// explicit reason when the sample is refused.
func (s *Store) Ingest(sample model.Sample) error {
	if sample.Name == "" {
		return &model.RejectError{Reason: model.RejectInvalid, Detail: "empty metric name"}
	}
	if sample.Timestamp.IsZero() {
		return &model.RejectError{Reason: model.RejectInvalid, Detail: "zero timestamp"}
	}
	key := model.MakeSeriesKey(sample.Name, sample.Labels)
	wStart := sample.Timestamp.Truncate(s.opt.WindowSize)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Advance / check the watermark first so a stale sample cannot
	// move it backwards.
	if !s.latestWindow.IsZero() {
		oldest := s.latestWindow.Add(-time.Duration(s.opt.StaleWindows) * s.opt.WindowSize)
		if wStart.Before(oldest) {
			return &model.RejectError{
				Reason: model.RejectStale,
				Detail: "sample window " + wStart.Format(time.RFC3339) +
					" is older than accepted bound " + oldest.Format(time.RFC3339) +
					" for series " + string(key),
			}
		}
	}
	if wStart.After(s.latestWindow) {
		s.latestWindow = wStart
	}

	st, ok := s.series[key]
	if !ok {
		if len(s.series) >= s.opt.MaxSeries {
			return &model.RejectError{
				Reason: model.RejectCardinality,
				Detail: "new series " + string(key) + " rejected; at cardinality limit " +
					strconv.Itoa(s.opt.MaxSeries),
			}
		}
		labels := make(map[string]string, len(sample.Labels))
		for k, v := range sample.Labels {
			labels[k] = v
		}
		st = &seriesState{
			name:    sample.Name,
			labels:  labels,
			windows: make(map[time.Time]*windowAgg),
		}
		s.series[key] = st
	}

	w, ok := st.windows[wStart]
	if !ok {
		w = &windowAgg{values: make(map[time.Time]float64)}
		st.windows[wStart] = w
	}
	// Last-write-wins on identical timestamps: assignment replaces the
	// value, never adds a second count.
	w.values[sample.Timestamp] = sample.Value

	s.gcLocked(wStart)
	return nil
}

// gcLocked bounds per-series memory. Windows are delivered data and
// are ONLY removed by Flush; this GC never deletes a window that has
// not been flushed yet. It only evicts the rare stragglers sitting far
// beyond the stale bound (for example a series that stopped receiving
// any samples), whose late samples would already be rejected as stale.
func (s *Store) gcLocked(arrivalWindow time.Time) {
	arrivalEnd := arrivalWindow.Add(s.opt.WindowSize)
	// A window this old has been deliverable for at least one full
	// window past the stale bound; keeping it only leaks memory.
	safety := time.Duration(s.opt.StaleWindows+1) * s.opt.WindowSize
	for key, st := range s.series {
		for ws := range st.windows {
			expired := ws.Add(s.opt.WindowSize).Add(safety)
			if !expired.After(arrivalEnd) { // expired <= arrivalEnd
				delete(st.windows, ws)
			}
		}
		if len(st.windows) == 0 {
			delete(s.series, key)
		}
	}
}

// Flush returns and removes every aggregate whose window is closed and
// already too old to receive an in-policy late sample at time now:
// windowEnd + StaleWindows*WindowSize <= now. Results are unordered;
// callers must not assume ordering across series/windows.
func (s *Store) Flush(now time.Time) []model.WindowAggregate {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []model.WindowAggregate
	for key, st := range s.series {
		for ws, w := range st.windows {
			wEnd := ws.Add(s.opt.WindowSize)
			readyAt := wEnd.Add(time.Duration(s.opt.StaleWindows) * s.opt.WindowSize)
			if !readyAt.After(now) { // readyAt <= now
				out = append(out, buildAggregate(key, st, ws, wEnd, w))
				delete(st.windows, ws)
			}
		}
	}
	return out
}

func buildAggregate(key model.SeriesKey, st *seriesState, start, end time.Time, w *windowAgg) model.WindowAggregate {
	agg := model.WindowAggregate{
		Series:      key,
		Name:        st.name,
		Labels:      make(map[string]string, len(st.labels)),
		WindowStart: start,
		WindowEnd:   end,
		Count:       int64(len(w.values)),
		Min:         math.Inf(1),
		Max:         math.Inf(-1),
	}
	for k, v := range st.labels {
		agg.Labels[k] = v
	}
	var latestTS time.Time
	for ts, v := range w.values {
		agg.Sum += v
		if v < agg.Min {
			agg.Min = v
		}
		if v > agg.Max {
			agg.Max = v
		}
		if ts.After(latestTS) {
			latestTS = ts
			agg.Latest = v
		}
	}
	return agg
}

// SeriesCount reports the number of distinct series currently tracked.
func (s *Store) SeriesCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.series)
}

// LatestWindow returns the newest observed window start.
func (s *Store) LatestWindow() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latestWindow
}

// Close releases background resources (none today).
func (s *Store) Close() {}
