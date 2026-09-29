package rule

import (
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

func agg() model.WindowAggregate {
	return model.WindowAggregate{
		Name:   "cpu",
		Labels: map[string]string{"host": "a", "zone": "z1"},
		Count:  2, Sum: 6, Min: 1, Max: 5, Latest: 5,
	}
}

func TestMatches(t *testing.T) {
	r := Rule{ID: "r1", Metric: "cpu", Op: OpGT, Threshold: 2, Field: FieldAvg}
	if !r.Matches(agg()) { // avg 3 > 2
		t.Fatal("avg match failed")
	}
	r.Field = FieldLatest
	if !r.Matches(agg()) { // latest 5 > 2
		t.Fatal("latest match failed")
	}
	r.Threshold = 5
	if r.Matches(agg()) { // 5 > 5 false
		t.Fatal("strict gt failed")
	}
	r.Op = OpGE
	if !r.Matches(agg()) {
		t.Fatal("ge failed")
	}
	r.LabelFilter = map[string]string{"host": "b"}
	if r.Matches(agg()) {
		t.Fatal("label filter should fail")
	}
	r.LabelFilter = map[string]string{"host": "a"}
	if !r.Matches(agg()) {
		t.Fatal("label filter should match")
	}
	r.Metric = "mem"
	if r.Matches(agg()) {
		t.Fatal("metric mismatch matched")
	}
}

func TestValidate(t *testing.T) {
	bad := []Rule{
		{Metric: "m"},
		{ID: "x", Metric: "m", Op: "??"},
		{ID: "x", Metric: "m", Op: OpGT, Field: "??"},
		{ID: "x", Metric: "m", Op: OpGT, ForDuration: -time.Second},
	}
	for i, r := range bad {
		if err := r.Validate(); err == nil {
			t.Fatalf("case %d expected error", i)
		}
	}
	snap := Snapshot{Rules: []Rule{
		{ID: "a", Metric: "m", Op: OpGT},
		{ID: "a", Metric: "m", Op: OpLT},
	}}
	if err := snap.Validate(); err == nil {
		t.Fatal("duplicate ids accepted")
	}
}
