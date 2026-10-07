package engine

import (
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
)

func aggAt(name, host string, v float64, sec int64) model.WindowAggregate {
	labels := map[string]string{"host": host}
	return model.WindowAggregate{
		Series:      model.MakeSeriesKey(name, labels),
		Name:        name,
		Labels:      labels,
		WindowStart: time.Unix(sec, 0),
		WindowEnd:   time.Unix(sec+1, 0),
		Count:       1, Sum: v, Min: v, Max: v, Latest: v,
	}
}

func hotRules() rule.Snapshot {
	return rule.Snapshot{Version: 1, Rules: []rule.Rule{{
		ID: "r1", Metric: "cpu", Field: rule.FieldAvg, Op: rule.OpGT,
		Threshold: 5, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second,
	}}}
}

func TestExportRestoreRoundtripKeepsEpisode(t *testing.T) {
	t.Log("场景: 引擎状态导出/恢复后, firing episode 延续, 不会重复发 Firing")
	e1 := New(hotRules())
	// Drive to firing: hot windows ending at 1,2,3.
	var fired []model.AlertEvent
	for sec := int64(0); sec <= 2; sec++ {
		fired = append(fired, e1.Evaluate([]model.WindowAggregate{aggAt("cpu", "a", 9, sec)})...)
	}
	if len(fired) != 1 || fired[0].State != model.StateFiring {
		t.Fatalf("setup: want 1 firing, got %+v", fired)
	}
	recs := e1.Export()
	if len(recs) != 1 || recs[0].State != "firing" || !recs[0].Episode {
		t.Fatalf("export must carry the open episode, got %+v", recs)
	}

	e2 := New(hotRules())
	rep := e2.Restore(recs)
	if rep.Restored != 1 || len(rep.DroppedRule) != 0 {
		t.Fatalf("restore report: %+v", rep)
	}
	// Flap and resolve on the restored engine: no second Firing,
	// exactly one Resolved carrying the original FiredAt.
	var evs []model.AlertEvent
	evs = append(evs, e2.Evaluate([]model.WindowAggregate{aggAt("cpu", "a", 1, 3)})...)
	evs = append(evs, e2.Evaluate([]model.WindowAggregate{aggAt("cpu", "a", 9, 4)})...)
	evs = append(evs, e2.Evaluate([]model.WindowAggregate{aggAt("cpu", "a", 1, 5)})...)
	evs = append(evs, e2.Evaluate([]model.WindowAggregate{aggAt("cpu", "a", 1, 6)})...)
	evs = append(evs, e2.Evaluate([]model.WindowAggregate{aggAt("cpu", "a", 1, 7)})...)
	if len(evs) != 1 || evs[0].State != model.StateInactive {
		t.Fatalf("want exactly 1 resolved event after restore, got %+v", evs)
	}
	if !evs[0].FiredAt.Equal(fired[0].FiredAt) {
		t.Fatalf("resolved event must reference the original episode start %v, got %v",
			fired[0].FiredAt, evs[0].FiredAt)
	}
}

func TestRestoreDropsDeletedRules(t *testing.T) {
	t.Log("场景: 恢复时规则已被删除, 状态被丢弃并在报告中列明, 不参与后续判定")
	e1 := New(hotRules())
	e1.Evaluate([]model.WindowAggregate{aggAt("cpu", "a", 9, 0)})
	recs := e1.Export()
	if len(recs) != 1 {
		t.Fatalf("want 1 exported record, got %d", len(recs))
	}

	// New engine with an unrelated ruleset: r1 is gone.
	e2 := New(rule.Snapshot{Version: 2, Rules: []rule.Rule{{
		ID: "r2", Metric: "mem", Field: rule.FieldAvg, Op: rule.OpGT,
		Threshold: 5, ForDuration: time.Second, KeepResolvedFor: time.Second,
	}}})
	rep := e2.Restore(recs)
	if rep.Restored != 0 || len(rep.DroppedRule) != 1 || rep.DroppedRule[0] != "r1" {
		t.Fatalf("deleted rule must be reported dropped, got %+v", rep)
	}
	if got := len(e2.Snapshot()); got != 0 {
		t.Fatalf("dropped state must not appear in the engine, got %d states", got)
	}
}

func TestExportSkipsPlainInactive(t *testing.T) {
	t.Log("场景: 纯 inactive 状态不导出(与干净状态等价), 检查点不膨胀")
	e := New(hotRules())
	e.Evaluate([]model.WindowAggregate{aggAt("cpu", "a", 1, 0)}) // cold: inactive
	if got := len(e.Export()); got != 0 {
		t.Fatalf("inactive state carries no information, want 0 records, got %d", got)
	}
}
