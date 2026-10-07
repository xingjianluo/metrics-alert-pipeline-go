package engine

import (
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/checkpoint"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// RestoreReport summarizes a Restore for explainable logging.
type RestoreReport struct {
	Restored    int      // alert states continued
	DroppedRule []string // rule IDs in the checkpoint but not in the current ruleset
}

// Export copies all live alert states into restorable records. Only
// states worth continuing are exported: a plain inactive state with no
// open episode carries no information a fresh state would not.
func (e *Engine) Export() []checkpoint.AlertRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []checkpoint.AlertRecord
	for rid, bucket := range e.states {
		for k, st := range bucket {
			if st.state == model.StateInactive && !st.episode {
				continue
			}
			out = append(out, checkpoint.AlertRecord{
				RuleID:  rid,
				Series:  string(k),
				State:   st.state.String(),
				Since:   st.since,
				FiredAt: st.firedAt,
				Episode: st.episode,
			})
		}
	}
	return out
}

// Restore installs checkpointed alert states, applying the same
// rule-matching semantics as a hot update: states whose rule ID exists
// in the current ruleset continue (pending timers, firing episodes and
// resolving timers are preserved, so nothing re-fires or re-resolves);
// states of deleted rules are dropped and reported; rules without
// checkpointed state simply start clean.
func (e *Engine) Restore(recs []checkpoint.AlertRecord) RestoreReport {
	snap := e.rules.Load()
	valid := make(map[string]bool, len(snap.Rules))
	for _, r := range snap.Rules {
		valid[r.ID] = true
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	rep := RestoreReport{}
	dropped := map[string]bool{}
	for _, rec := range recs {
		if !valid[rec.RuleID] {
			// Rule deleted since the checkpoint was written: the state
			// no longer participates.
			if !dropped[rec.RuleID] {
				dropped[rec.RuleID] = true
				rep.DroppedRule = append(rep.DroppedRule, rec.RuleID)
			}
			continue
		}
		bucket, ok := e.states[rec.RuleID]
		if !ok {
			bucket = make(map[model.SeriesKey]*alertState)
			e.states[rec.RuleID] = bucket
		}
		bucket[model.SeriesKey(rec.Series)] = &alertState{
			state:   parseState(rec.State),
			since:   rec.Since,
			firedAt: rec.FiredAt,
			episode: rec.Episode,
		}
		rep.Restored++
	}
	return rep
}

func parseState(s string) model.AlertState {
	switch s {
	case "pending":
		return model.StatePending
	case "firing":
		return model.StateFiring
	case "resolving":
		return model.StateResolving
	default:
		return model.StateInactive
	}
}
