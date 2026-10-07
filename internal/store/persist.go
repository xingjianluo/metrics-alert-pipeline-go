package store

import (
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/checkpoint"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// Export copies every unflushed window plus the staleness watermark
// into restorable records.
func (s *Store) Export() (recs []checkpoint.WindowRecord, latest time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for key, st := range s.series {
		for ws, w := range st.windows {
			rec := checkpoint.WindowRecord{
				Series: string(key),
				Name:   st.name,
				Labels: make(map[string]string, len(st.labels)),
				Start:  ws,
				Values: make(map[time.Time]float64, len(w.values)),
			}
			for k, v := range st.labels {
				rec.Labels[k] = v
			}
			for ts, v := range w.values {
				rec.Values[ts] = v
			}
			recs = append(recs, rec)
		}
	}
	return recs, s.latestWindow
}

// Restore installs checkpointed windows and the watermark, replacing
// any current content. Restored windows flush by the normal readiness
// rule (windowEnd + StaleWindows*WindowSize <= now), so evaluation
// simply continues where the checkpoint left off. The cardinality cap
// is not re-applied to restored data: it bounds new ingests, and
// refusing already-accepted windows would silently drop data.
func (s *Store) Restore(recs []checkpoint.WindowRecord, latest time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.series = make(map[model.SeriesKey]*seriesState)
	s.latestWindow = latest
	for _, rec := range recs {
		key := model.SeriesKey(rec.Series)
		st, ok := s.series[key]
		if !ok {
			labels := make(map[string]string, len(rec.Labels))
			for k, v := range rec.Labels {
				labels[k] = v
			}
			st = &seriesState{
				name:    rec.Name,
				labels:  labels,
				windows: make(map[time.Time]*windowAgg),
			}
			s.series[key] = st
		}
		values := make(map[time.Time]float64, len(rec.Values))
		for ts, v := range rec.Values {
			values[ts] = v
		}
		st.windows[rec.Start] = &windowAgg{values: values}
	}
}
