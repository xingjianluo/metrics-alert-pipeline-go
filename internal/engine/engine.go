// Package engine maintains per (rule, series) alert state, drives the
// state machine, deduplicates firing events and supports atomic rule
// hot-updates that preserve in-flight pending/firing state.
//
// State machine (one alert identity = rule ID + canonical series key):
//
//	inactive --condition true------------> pending
//	pending  --true for ForDuration------> firing   (emit Firing, once)
//	pending  --condition false-----------> inactive (pending discarded)
//	firing   --condition false-----------> resolving
//	resolving--false for KeepResolvedFor-> inactive (emit Resolved)
//	resolving--condition true------------> firing   (no new event:
//	                                              same active episode)
//
// A firing "episode" starts at the first Firing event and ends at the
// Resolved event. Any number of flaps inside an episode never produces
// a second Firing event: duplicate events are collapsed by the
// (rule, series, episode) identity.
package engine

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
)

type alertState struct {
	state   model.AlertState
	since   time.Time // start of the current true/false run
	firedAt time.Time // first firing time of the current episode
	episode bool      // a firing episode is active and already announced
}

// Engine evaluates window aggregates against the current ruleset.
type Engine struct {
	rules atomic.Pointer[rule.Snapshot]

	mu     sync.Mutex
	states map[string]map[model.SeriesKey]*alertState // ruleID -> series -> state
}

// New constructs an Engine holding the given ruleset snapshot.
func New(snap rule.Snapshot) *Engine {
	e := &Engine{states: make(map[string]map[model.SeriesKey]*alertState)}
	e.rules.Store(&snap)
	return e
}

// UpdateRules atomically swaps the ruleset. States of rules whose ID is
// present in both the old and new snapshot are preserved untouched
// (pending timers and firing episodes neither reset nor double-count).
// States belonging only to deleted rules are retired. New rules start
// with empty state. The swap itself is serialized with evaluation.
func (e *Engine) UpdateRules(snap rule.Snapshot) error {
	if err := snap.Validate(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	next := make(map[string]map[model.SeriesKey]*alertState, len(snap.Rules))
	for _, r := range snap.Rules {
		// Preserve existing state keyed by rule ID.
		if old, ok := e.states[r.ID]; ok {
			next[r.ID] = old
		} else {
			next[r.ID] = make(map[model.SeriesKey]*alertState)
		}
	}
	e.states = next
	e.rules.Store(&snap)
	return nil
}

// Rules returns the current ruleset snapshot.
func (e *Engine) Rules() rule.Snapshot { return *e.rules.Load() }

// Evaluate feeds one batch of window aggregates through every rule.
// Aggregates are applied in (series, windowStart) order so batched
// windows after a stall are processed deterministically. It returns
// the alert events emitted on this call (Firing / Resolved only).
func (e *Engine) Evaluate(aggs []model.WindowAggregate) []model.AlertEvent {
	snap := e.rules.Load()
	rulesByID := make(map[string]rule.Rule, len(snap.Rules))
	for _, r := range snap.Rules {
		rulesByID[r.ID] = r
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	var events []model.AlertEvent
	for _, r := range snap.Rules {
		bucket, ok := e.states[r.ID]
		if !ok {
			bucket = make(map[model.SeriesKey]*alertState)
			e.states[r.ID] = bucket
		}
		// Pick this rule's aggregates and order them by window start.
		var items []evalItem
		for _, a := range aggs {
			if a.Name != r.Metric {
				continue
			}
			items = append(items, evalItem{agg: a, hit: r.Matches(a)})
		}
		sortItems(items)
		for _, it := range items {
			if ev := e.advance(r, bucket, it.agg, it.hit); ev != nil {
				events = append(events, *ev)
			}
		}
	}
	return events
}

func (e *Engine) advance(r rule.Rule, bucket map[model.SeriesKey]*alertState,
	agg model.WindowAggregate, cond bool) *model.AlertEvent {
	at := agg.WindowEnd
	st, ok := bucket[agg.Series]
	if !ok {
		st = &alertState{state: model.StateInactive, since: at}
		bucket[agg.Series] = st
	}

	var emit *model.AlertEvent
	mkEvent := func(state model.AlertState) model.AlertEvent {
		labels := make(map[string]string, len(agg.Labels))
		for k, v := range agg.Labels {
			labels[k] = v
		}
		return model.AlertEvent{
			RuleID: r.ID, Series: agg.Series, Name: agg.Name, Labels: labels,
			State: state, Value: valueForLog(agg, string(r.Field)), EvalTime: at,
		}
	}

	switch st.state {
	case model.StateInactive:
		if cond {
			st.state = model.StatePending
			st.since = at
		}
	case model.StatePending:
		if cond {
			if at.Sub(st.since) >= r.ForDuration {
				st.state = model.StateFiring
				st.firedAt = at
				st.episode = true
				ev := mkEvent(model.StateFiring)
				ev.FiredAt = at
				emit = &ev
			}
		} else {
			st.state = model.StateInactive
			st.since = at
		}
	case model.StateFiring:
		if !cond {
			st.state = model.StateResolving
			st.since = at
		}
	case model.StateResolving:
		if cond {
			// Flap back while the episode is still open: return to
			// firing but do NOT emit another Firing event.
			st.state = model.StateFiring
		} else if at.Sub(st.since) >= r.KeepResolvedFor {
			st.state = model.StateInactive
			st.episode = false
			ev := mkEvent(model.StateInactive)
			ev.ResolvedAt = at
			ev.FiredAt = st.firedAt
			emit = &ev
		}
	}
	return emit
}

// StateView is an external read-only view of one alert's state.
type StateView struct {
	RuleID  string
	Series  model.SeriesKey
	State   model.AlertState
	Since   time.Time
	FiredAt time.Time
}

// Snapshot returns a point-in-time copy of all alert states.
func (e *Engine) Snapshot() []StateView {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []StateView
	for rid, bucket := range e.states {
		for k, st := range bucket {
			out = append(out, StateView{
				RuleID: rid, Series: k, State: st.state,
				Since: st.since, FiredAt: st.firedAt,
			})
		}
	}
	return out
}

// Close releases engine resources.
func (e *Engine) Close() {}
