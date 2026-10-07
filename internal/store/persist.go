package store

import (
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// PersistedWindow is one not-yet-flushed aggregation window: the exact
// per-timestamp values, so duplicate-merging semantics survive a
// restart unchanged.
type PersistedWindow struct {
	Start  time.Time
	Values map[time.Time]float64
}

// PersistedSeries is one series with its pending windows.
type PersistedSeries struct {
	Key     model.SeriesKey
	Name    string
	Labels  map[string]string
	Windows []PersistedWindow
}

// PersistedStore is the serializable form of the whole store: every
// unflushed window plus the stale-bound watermark.
type PersistedStore struct {
	LatestWindow time.Time
	Series       []PersistedSeries
}

// Export copies all unflushed windows and the watermark.
func (s *Store) Export() PersistedStore {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := PersistedStore{LatestWindow: s.latestWindow}
	for key, st := range s.series {
		ps := PersistedSeries{
			Key:    key,
			Name:   st.name,
			Labels: make(map[string]string, len(st.labels)),
		}
		for k, v := range st.labels {
			ps.Labels[k] = v
		}
		for ws, w := range st.windows {
			pw := PersistedWindow{Start: ws, Values: make(map[time.Time]float64, len(w.values))}
			for ts, v := range w.values {
				pw.Values[ts] = v
			}
			ps.Windows = append(ps.Windows, pw)
		}
		out.Series = append(out.Series, ps)
	}
	return out
}

// Restore replaces the store content with persisted data. Series beyond
// the cardinality cap are skipped (reported in the return value) so a
// checkpoint written under a larger cap cannot silently exceed the
// current one. Call before the first Ingest/Flush.
func (s *Store) Restore(ps PersistedStore) (skippedSeries int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[model.SeriesKey]*seriesState, len(ps.Series))
	for _, sp := range ps.Series {
		if len(next) >= s.opt.MaxSeries {
			skippedSeries++
			continue
		}
		st := &seriesState{
			name:    sp.Name,
			labels:  make(map[string]string, len(sp.Labels)),
			windows: make(map[time.Time]*windowAgg, len(sp.Windows)),
		}
		for k, v := range sp.Labels {
			st.labels[k] = v
		}
		for _, w := range sp.Windows {
			vals := make(map[time.Time]float64, len(w.Values))
			for ts, v := range w.Values {
				vals[ts] = v
			}
			st.windows[w.Start] = &windowAgg{values: vals}
		}
		next[sp.Key] = st
	}
	s.series = next
	s.latestWindow = ps.LatestWindow
	return skippedSeries
}
