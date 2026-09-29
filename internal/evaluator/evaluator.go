// Package evaluator evaluates windowed aggregates against alerting rules and
// maintains a per-alert state machine. Rules can be updated at runtime without
// resetting pending/firing state; transition events are produced only on real
// state changes, so a flapping alert is never counted twice for one episode.
package evaluator

import (
	"log"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// alertInstance is the mutable state of one (rule, series) alert.
type alertInstance struct {
	ruleID string
	key    string
	labels map[string]string

	state          model.AlertState
	violationSince time.Time // start of the current continuous violation run
	recoverSince   time.Time // start of the current continuous recovery run
	since          time.Time // when the current state was entered
	lastValue      float64
}

// Evaluator holds rules and alert states.
type Evaluator struct {
	mu     sync.Mutex
	rules  map[string]model.Rule
	alerts map[string]*alertInstance // alert key -> instance
	logger *log.Logger
}

// New creates an Evaluator seeded with rules.
func New(rules []model.Rule) *Evaluator {
	e := &Evaluator{
		rules:  make(map[string]model.Rule),
		alerts: make(map[string]*alertInstance),
		logger: log.Default(),
	}
	for _, r := range rules {
		e.rules[r.ID] = r
	}
	return e
}

// SetLogger replaces the structured decision logger (nil disables logging).
func (e *Evaluator) SetLogger(l *log.Logger) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logger = l
}

// UpdateRules hot-replaces the rule set keyed by Rule.ID.
// Instances of unchanged rule IDs keep their pending/firing state and timers;
// new rules start clean; removed rules resolve any still-active alerts.
func (e *Evaluator) UpdateRules(rules []model.Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()

	next := make(map[string]model.Rule, len(rules))
	for _, r := range rules {
		next[r.ID] = r
	}
	var resolved []model.Event
	now := time.Now()
	for id := range e.rules {
		if _, kept := next[id]; !kept {
			for key, inst := range e.alerts {
				if inst.ruleID != id {
					continue
				}
				if inst.state == model.StateFiring || inst.state == model.StateRecovering ||
					inst.state == model.StatePending {
					resolved = append(resolved, model.Event{
						RuleID: inst.ruleID, Key: key,
						Labels: model.CloneLabels(inst.labels),
						Type:   model.EventResolved, Value: inst.lastValue,
						OccurredAt: now,
					})
				}
				delete(e.alerts, key)
			}
		}
	}
	e.rules = next
	e.logf("rules updated: %d active, %d alerts force-resolved by rule removal",
		len(next), len(resolved))
}

// Evaluate applies one window aggregate to every matching rule and returns the
// transition events it caused (usually zero or one). The input window and the
// resulting decision are logged for explainability.
func (e *Evaluator) Evaluate(agg model.Aggregate) []model.Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	var events []model.Event
	ruleIDs := make([]string, 0, len(e.rules))
	for id := range e.rules {
		ruleIDs = append(ruleIDs, id)
	}
	sort.Strings(ruleIDs)

	seriesKey := model.Fingerprint(agg.Name, agg.Labels)
	for _, id := range ruleIDs {
		r := e.rules[id]
		if r.Metric != agg.Name || !model.MatchLabels(agg.Labels, r.MatchLabels) {
			continue
		}
		val, violating := r.Violating(agg)
		key := model.AlertKey(r.ID, seriesKey)
		// Timing convention: an aggregate for [WindowStart, WindowEnd) means
		// the condition held for that whole window, so a streak starts at
		// WindowStart and its elapsed length is measured at WindowEnd.
		ev := e.apply(key, r, agg.Labels, val, violating, agg.WindowStart, agg.WindowEnd)
		events = append(events, ev...)
		e.logf("evaluate input={rule=%s metric=%s window=[%s,%s) labels=%v agg=%s value=%.4f violating=%t} result={key=%s events=%d}",
			r.ID, agg.Name,
			agg.WindowStart.Format(time.RFC3339Nano), agg.WindowEnd.Format(time.RFC3339Nano),
			agg.Labels, r.Agg, val, violating, key, len(ev))
	}
	return events
}

// apply runs one (rule, series) state-machine transition. streakStart is the
// beginning of the aggregate window (when the condition began to hold for it);
// at is the evaluation time (window end). Caller holds mu.
func (e *Evaluator) apply(key string, r model.Rule, labels map[string]string,
	value float64, violating bool, streakStart, at time.Time) []model.Event {

	inst := e.alerts[key]
	if inst == nil {
		if !violating {
			return nil
		}
		inst = &alertInstance{
			ruleID: r.ID, key: key,
			labels: model.CloneLabels(labels),
		}
		e.alerts[key] = inst
	}
	inst.lastValue = value

	var events []model.Event
	switch inst.state {
	case model.StateInactive, "":
		if violating {
			inst.enter(model.StatePending, at)
			inst.violationSince = streakStart
			events = append(events, e.event(inst, model.EventPending, at))
		}
	case model.StatePending:
		switch {
		case violating:
			if at.Sub(inst.violationSince) >= r.ForDuration {
				inst.enter(model.StateFiring, at)
				events = append(events, e.event(inst, model.EventFired, at))
			}
		default:
			// Violation interrupted before the For-duration elapsed: reset.
			inst.enter(model.StateInactive, at)
			inst.violationSince = time.Time{}
			delete(e.alerts, key)
		}
	case model.StateFiring:
		if !violating {
			inst.enter(model.StateRecovering, at)
			inst.recoverSince = streakStart
			events = append(events, e.event(inst, model.EventRecovering, at))
		}
		// Continued violation emits nothing: fired events deduplicated.
	case model.StateRecovering:
		if violating {
			// Flapped back before recovery completed. No new fired event:
			// the episode was never resolved, so it cannot fire twice.
			inst.enter(model.StateFiring, at)
		} else if at.Sub(inst.recoverSince) >= r.KeepDuration {
			events = append(events, e.event(inst, model.EventResolved, at))
			delete(e.alerts, key)
		}
	}
	return events
}

func (inst *alertInstance) enter(s model.AlertState, now time.Time) {
	inst.state = s
	inst.since = now
}

func (e *Evaluator) event(inst *alertInstance, t model.EventType, now time.Time) model.Event {
	return model.Event{
		RuleID:     inst.ruleID,
		Key:        inst.key,
		Labels:     model.CloneLabels(inst.labels),
		Type:       t,
		Value:      inst.lastValue,
		OccurredAt: now,
	}
}

// Tick advances time-driven transitions for instances that received no window
// in this period (notably recovering alerts whose keep-duration elapses while
// the series stops reporting). Rule removal is handled by UpdateRules.
func (e *Evaluator) Tick(now time.Time) []model.Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	var events []model.Event
	for key, inst := range e.alerts {
		r := e.rules[inst.ruleID]
		if inst.state == model.StateRecovering && now.Sub(inst.recoverSince) >= r.KeepDuration {
			events = append(events, e.event(inst, model.EventResolved, now))
			delete(e.alerts, key)
		}
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Key < events[j].Key })
	return events
}

// AlertStatus is an introspectable view of one alert instance.
type AlertStatus struct {
	RuleID    string
	Key       string
	Labels    map[string]string
	State     model.AlertState
	Since     time.Time
	LastValue float64
}

// Snapshot returns a point-in-time copy of alert states (tests/introspection).
func (e *Evaluator) Snapshot() map[string]AlertStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]AlertStatus, len(e.alerts))
	for k, inst := range e.alerts {
		out[k] = AlertStatus{
			RuleID: inst.ruleID, Key: inst.key,
			Labels: model.CloneLabels(inst.labels),
			State:  inst.state, Since: inst.since, LastValue: inst.lastValue,
		}
	}
	return out
}

func (e *Evaluator) logf(format string, args ...any) {
	if e.logger != nil {
		e.logger.Printf("[evaluator] "+format, args...)
	}
}
