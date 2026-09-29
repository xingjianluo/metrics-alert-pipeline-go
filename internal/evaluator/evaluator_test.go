package evaluator

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

func t0() time.Time { return time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC) }

func agg(metric string, host string, end time.Time, value float64) model.Aggregate {
	return model.Aggregate{
		Name:        metric,
		Labels:      map[string]string{"host": host},
		WindowStart: end.Add(-time.Minute),
		WindowEnd:   end,
		Count:       1,
		Sum:         value,
		Min:         value,
		Max:         value,
		Last:        value,
	}
}

func cpuRule() model.Rule {
	return model.Rule{
		ID: "r-cpu", Metric: "cpu",
		Agg: model.AggLast, Op: model.OpGT, Threshold: 80,
		Window: time.Minute, ForDuration: 2 * time.Minute, KeepDuration: 2 * time.Minute,
	}
}

func typesOf(evs []model.Event) []model.EventType {
	out := make([]model.EventType, len(evs))
	for i, e := range evs {
		out[i] = e.Type
	}
	return out
}

func eqTypes(got []model.EventType, want ...model.EventType) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestFullStateMachineFlow(t *testing.T) {
	e := New([]model.Rule{cpuRule()})
	now := t0()
	key := "r-cpu|cpu{host=h1}"

	step := func(v float64, advance time.Duration) []model.EventType {
		now = now.Add(advance)
		return typesOf(e.Evaluate(agg("cpu", "h1", now, v)))
	}

	// healthy window: nothing
	if evs := step(10, time.Minute); len(evs) != 0 {
		t.Fatalf("healthy window produced events: %v", evs)
	}
	// first violation -> pending
	if evs := step(90, time.Minute); !eqTypes(evs, model.EventPending) {
		t.Fatalf("want pending, got %v", evs)
	}
	if s := e.Snapshot()[key]; s.State != model.StatePending {
		t.Fatalf("want pending state, got %s", s.State)
	}
	// one more violating minute: ForDuration (2m since violation start) met -> fired
	if evs := step(91, time.Minute); !eqTypes(evs, model.EventFired) {
		t.Fatalf("want fired, got %v", evs)
	}
	// continuing violation: NO duplicate fired event
	if evs := step(92, time.Minute); len(evs) != 0 {
		t.Fatalf("continuous firing must not re-emit, got %v", evs)
	}
	// first healthy window while firing -> recovering
	if evs := step(5, time.Minute); !eqTypes(evs, model.EventRecovering) {
		t.Fatalf("want recovering, got %v", evs)
	}
	// one more healthy minute: keep duration met -> resolved
	if evs := step(4, time.Minute); !eqTypes(evs, model.EventResolved) {
		t.Fatalf("want resolved, got %v", evs)
	}
	if _, ok := e.Snapshot()[key]; ok {
		t.Fatal("resolved instance must be removed")
	}
}

func TestPendingInterruptResets(t *testing.T) {
	e := New([]model.Rule{cpuRule()})
	now := t0()
	now = now.Add(time.Minute)
	if evs := e.Evaluate(agg("cpu", "h1", now, 90)); !eqTypes(typesOf(evs), model.EventPending) {
		t.Fatalf("want pending, got %v", evs)
	}
	now = now.Add(time.Minute)
	if evs := e.Evaluate(agg("cpu", "h1", now, 10)); len(evs) != 0 {
		t.Fatalf("interrupted pending must silently reset, got %v", evs)
	}
	if _, ok := e.Snapshot()["r-cpu|cpu{host=h1}"]; ok {
		t.Fatal("interrupted pending instance must be cleared so the For timer restarts")
	}
	// violates again: must start a fresh pending with a fresh timer
	now = now.Add(time.Minute)
	if evs := e.Evaluate(agg("cpu", "h1", now, 90)); !eqTypes(typesOf(evs), model.EventPending) {
		t.Fatalf("want fresh pending after reset, got %v", evs)
	}
}

func TestFlapDoesNotDoubleFire(t *testing.T) {
	e := New([]model.Rule{cpuRule()})
	now := t0()
	var all []model.EventType
	feed := func(v float64) {
		now = now.Add(time.Minute)
		all = append(all, typesOf(e.Evaluate(agg("cpu", "h1", now, v)))...)
	}

	feed(90) // pending
	feed(90) // fired
	feed(5)  // recovering
	feed(90) // flap back BEFORE keep duration: same episode, no new event
	feed(5)  // recovering again, recovery timer restarts
	feed(5)  // resolved

	var fired, resolved int
	for _, ty := range all {
		if ty == model.EventFired {
			fired++
		}
		if ty == model.EventResolved {
			resolved++
		}
	}
	if fired != 1 {
		t.Fatalf("flapping must not duplicate fired events, got %d (sequence=%v)", fired, all)
	}
	if resolved != 1 {
		t.Fatalf("want exactly one resolution, got %d (sequence=%v)", resolved, all)
	}
}

func TestHotUpdatePreservesState(t *testing.T) {
	rule := cpuRule()
	e := New([]model.Rule{rule})
	var buf bytes.Buffer
	e.SetLogger(log.New(&buf, "", 0))

	now := t0().Add(time.Minute)
	e.Evaluate(agg("cpu", "h1", now, 90)) // pending
	now = now.Add(time.Minute)
	e.Evaluate(agg("cpu", "h1", now, 90)) // fired

	// Hot update: same rule ID, different threshold/timings. State and timers
	// must survive; the alert must not fire again.
	rule.Threshold = 50
	rule.ForDuration = 10 * time.Minute
	e.UpdateRules([]model.Rule{rule})

	s := e.Snapshot()["r-cpu|cpu{host=h1}"]
	if s.State != model.StateFiring {
		t.Fatalf("hot update must preserve firing state, got %s", s.State)
	}
	now = now.Add(time.Minute)
	if evs := e.Evaluate(agg("cpu", "h1", now, 90)); len(evs) != 0 {
		t.Fatalf("reloaded same rule must not duplicate events, got %v", evs)
	}

	// Adding a brand-new rule starts clean and does not touch existing alerts.
	e.UpdateRules([]model.Rule{rule, {
		ID: "r-mem", Metric: "mem", Agg: model.AggLast, Op: model.OpGT,
		Threshold: 1, Window: time.Minute, ForDuration: time.Minute, KeepDuration: time.Minute,
	}})
	if s := e.Snapshot()["r-cpu|cpu{host=h1}"]; s.State != model.StateFiring {
		t.Fatalf("adding rule must not reset existing state, got %s", s.State)
	}

	// Removing the rule force-resolves the still-firing alert exactly once.
	e.UpdateRules(nil)
	if len(e.Snapshot()) != 0 {
		t.Fatalf("removed rules must clear instances, got %v", e.Snapshot())
	}
	if n := strings.Count(buf.String(), "rules updated"); n < 3 {
		t.Fatalf("want update log lines, got:\n%s", buf.String())
	}
}

func TestDecisionLoggingShowsInputAndResult(t *testing.T) {
	var buf bytes.Buffer
	e := New([]model.Rule{cpuRule()})
	e.SetLogger(log.New(&buf, "", 0))
	now := t0().Add(time.Minute)
	e.Evaluate(agg("cpu", "h1", now, 90))

	logged := buf.String()
	for _, want := range []string{"evaluate input=", "rule=r-cpu", "violating=true", "result=", "events=1"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("decision log missing %q in:\n%s", want, logged)
		}
	}
	t.Log("decision log:\n" + logged)
}

func TestTickResolvesRecoveringWithoutData(t *testing.T) {
	e := New([]model.Rule{cpuRule()})
	now := t0().Add(time.Minute)
	e.Evaluate(agg("cpu", "h1", now, 90)) // pending
	now = now.Add(time.Minute)
	e.Evaluate(agg("cpu", "h1", now, 90)) // fired
	now = now.Add(time.Minute)
	e.Evaluate(agg("cpu", "h1", now, 5)) // recovering

	// Series stops reporting. The periodic tick must still resolve it.
	now = now.Add(3 * time.Minute)
	evs := e.Tick(now)
	if !eqTypes(typesOf(evs), model.EventResolved) {
		t.Fatalf("tick must resolve silent recovering alert, got %v", typesOf(evs))
	}
}
