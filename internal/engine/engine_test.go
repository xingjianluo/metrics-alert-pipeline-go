package engine

import (
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
)

// mkAgg builds one-second window aggregates for metric cpu, host a,
// starting at unix t0; v is the avg/latest value.
func mkAgg(t0 int64, v float64) model.WindowAggregate {
	return model.WindowAggregate{
		Name: "cpu", Labels: map[string]string{"host": "a"},
		Series:      "cpu|host=a",
		WindowStart: time.Unix(t0, 0), WindowEnd: time.Unix(t0+1, 0),
		Count: 1, Sum: v, Min: v, Max: v, Latest: v,
	}
}

func firingRule() rule.Rule {
	return rule.Rule{ID: "hot", Metric: "cpu", Op: rule.OpGT, Threshold: 5,
		Field: rule.FieldAvg, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second}
}

func TestPendingFiringResolve(t *testing.T) {
	e := New(rule.Snapshot{Version: 1, Rules: []rule.Rule{firingRule()}})
	// t=1..2 over threshold; ForDuration=2s measured between windowEnds.
	var events []model.AlertEvent
	events = append(events, e.Evaluate([]model.WindowAggregate{mkAgg(0, 9)})...)
	events = append(events, e.Evaluate([]model.WindowAggregate{mkAgg(1, 9)})...)
	events = append(events, e.Evaluate([]model.WindowAggregate{mkAgg(2, 9)})...)
	if len(events) != 1 || events[0].State != model.StateFiring {
		t.Fatalf("want 1 firing, got %+v", events)
	}
	// Two good windows then resolve.
	events = nil
	events = append(events, e.Evaluate([]model.WindowAggregate{mkAgg(3, 1)})...)
	events = append(events, e.Evaluate([]model.WindowAggregate{mkAgg(4, 1)})...)
	events = append(events, e.Evaluate([]model.WindowAggregate{mkAgg(5, 1)})...)
	if len(events) != 1 || events[0].State != model.StateInactive {
		t.Fatalf("want 1 resolved, got %+v", events)
	}
	if events[0].FiredAt.IsZero() || events[0].ResolvedAt.IsZero() {
		t.Fatal("resolved event must carry fired/resolved times")
	}
}

func TestPendingResetWhenConditionDrops(t *testing.T) {
	e := New(rule.Snapshot{Version: 1, Rules: []rule.Rule{firingRule()}})
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(0, 9)}) // pending
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(1, 1)}) // back to inactive
	states := e.Snapshot()
	if len(states) != 1 || states[0].State != model.StateInactive {
		t.Fatalf("pending should reset: %+v", states)
	}
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(2, 9)}) // pending again, since=3
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(3, 9)}) // only 1s -> still pending
	events := e.Evaluate([]model.WindowAggregate{mkAgg(4, 9)})
	if len(events) != 1 {
		t.Fatalf("firing expected after fresh run: %+v", events)
	}
}

func TestFlapDoesNotDuplicateFiring(t *testing.T) {
	e := New(rule.Snapshot{Version: 1, Rules: []rule.Rule{firingRule()}})
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(0, 9)})
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(1, 9)})
	first := e.Evaluate([]model.WindowAggregate{mkAgg(2, 9)})
	if len(first) != 1 {
		t.Fatalf("want firing: %+v", first)
	}
	// Bad one window (resolving), good one window (flap back): no event.
	if ev := e.Evaluate([]model.WindowAggregate{mkAgg(3, 1)}); len(ev) != 0 {
		t.Fatalf("resolving emits nothing: %+v", ev)
	}
	if ev := e.Evaluate([]model.WindowAggregate{mkAgg(4, 9)}); len(ev) != 0 {
		t.Fatalf("flap back must not re-emit firing: %+v", ev)
	}
	// Bad two windows: resolving timer must restart at t=6.
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(5, 1)})
	if ev := e.Evaluate([]model.WindowAggregate{mkAgg(6, 1)}); len(ev) != 0 {
		t.Fatalf("one bad window after flap must not resolve: %+v", ev)
	}
	resolved := e.Evaluate([]model.WindowAggregate{mkAgg(7, 1)})
	if len(resolved) != 1 || resolved[0].State != model.StateInactive {
		t.Fatalf("want resolved after 2 clean windows: %+v", resolved)
	}
	// New episode -> one new firing.
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(8, 9)})
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(9, 9)})
	again := e.Evaluate([]model.WindowAggregate{mkAgg(10, 9)})
	if len(again) != 1 {
		t.Fatalf("new episode must emit one firing: %+v", again)
	}
}

func TestHotUpdatePreservesState(t *testing.T) {
	e := New(rule.Snapshot{Version: 1, Rules: []rule.Rule{firingRule()}})
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(0, 9)}) // pending, since=1
	// Hot update: same ID, changed threshold (and anything else).
	updated := firingRule()
	updated.Threshold = 1
	if err := e.UpdateRules(rule.Snapshot{Version: 2, Rules: []rule.Rule{updated}}); err != nil {
		t.Fatal(err)
	}
	states := e.Snapshot()
	if len(states) != 1 || states[0].State != model.StatePending {
		t.Fatalf("pending state lost on hot update: %+v", states)
	}
	if !states[0].Since.Equal(time.Unix(1, 0)) {
		t.Fatalf("pending timer reset on hot update: %+v", states[0].Since)
	}
	_ = e.Evaluate([]model.WindowAggregate{mkAgg(1, 9)})
	events := e.Evaluate([]model.WindowAggregate{mkAgg(2, 9)})
	if len(events) != 1 {
		t.Fatalf("pending countdown must survive hot update: %+v", events)
	}
	// Delete the rule via hot update: its states retire.
	if err := e.UpdateRules(rule.Snapshot{Version: 3, Rules: nil}); err != nil {
		t.Fatal(err)
	}
	if len(e.Snapshot()) != 0 {
		t.Fatal("deleted rule states should be retired")
	}
}

func TestHotUpdateRejectsInvalid(t *testing.T) {
	e := New(rule.Snapshot{Version: 1, Rules: []rule.Rule{firingRule()}})
	bad := rule.Snapshot{Version: 2, Rules: []rule.Rule{{ID: "x"}}}
	if err := e.UpdateRules(bad); err == nil {
		t.Fatal("invalid ruleset accepted")
	}
	if e.Rules().Version != 1 {
		t.Fatal("failed update must not change active ruleset")
	}
}
