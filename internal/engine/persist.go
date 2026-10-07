package engine

import (
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// PersistedAlert is the serializable form of one (rule, series) alert
// state. Episode must survive restarts: it is what prevents an already
// announced firing episode from emitting a second Firing event after
// recovery.
type PersistedAlert struct {
	RuleID  string
	Series  model.SeriesKey
	State   model.AlertState
	Since   time.Time
	FiredAt time.Time
	Episode bool
}

// Export copies all live alert states into a serializable slice.
func (e *Engine) Export() []PersistedAlert {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []PersistedAlert
	for rid, bucket := range e.states {
		for k, st := range bucket {
			out = append(out, PersistedAlert{
				RuleID: rid, Series: k, State: st.state,
				Since: st.since, FiredAt: st.firedAt, Episode: st.episode,
			})
		}
	}
	return out
}

// Restore replaces the engine's states with the persisted ones,
// intersected with the CURRENT ruleset (same compatibility rule as a
// hot update): states whose rule ID is not in the current snapshot are
// retired and returned in dropped; rules without persisted state start
// clean. Call before the first Evaluate.
func (e *Engine) Restore(alerts []PersistedAlert) (dropped []PersistedAlert) {
	e.mu.Lock()
	defer e.mu.Unlock()
	snap := e.rules.Load()
	next := make(map[string]map[model.SeriesKey]*alertState, len(snap.Rules))
	for _, r := range snap.Rules {
		next[r.ID] = make(map[model.SeriesKey]*alertState)
	}
	for _, a := range alerts {
		bucket, ok := next[a.RuleID]
		if !ok {
			dropped = append(dropped, a)
			continue
		}
		bucket[a.Series] = &alertState{
			state: a.State, since: a.Since, firedAt: a.FiredAt, episode: a.Episode,
		}
	}
	e.states = next
	return dropped
}
