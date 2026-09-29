package model

import (
	"math"
	"testing"
)

func TestFingerprintOrderIndependentAndStable(t *testing.T) {
	a := map[string]string{"zone": "z1", "host": "h1"}
	b := map[string]string{"host": "h1", "zone": "z1"}
	if Fingerprint("cpu", a) != Fingerprint("cpu", b) {
		t.Fatal("fingerprint must be independent of label map order")
	}
	if Fingerprint("cpu", a) == Fingerprint("mem", a) {
		t.Fatal("different metric names must differ")
	}
	if Fingerprint("cpu", a) == Fingerprint("cpu", map[string]string{"host": "h1"}) {
		t.Fatal("different label sets must differ")
	}
	if got := Fingerprint("cpu", map[string]string{"host": "h1"}); got != "cpu{host=h1}" {
		t.Fatalf("canonical form = %q", got)
	}
}

func TestMatchLabels(t *testing.T) {
	labels := map[string]string{"host": "h1", "zone": "z1"}
	if !MatchLabels(labels, nil) || !MatchLabels(labels, map[string]string{"host": "h1"}) {
		t.Fatal("empty/subsets must match")
	}
	if MatchLabels(labels, map[string]string{"host": "h2"}) {
		t.Fatal("different value must not match")
	}
	if MatchLabels(labels, map[string]string{"env": "prod"}) {
		t.Fatal("missing key must not match")
	}
}

func TestAggregateValue(t *testing.T) {
	a := Aggregate{Count: 4, Sum: 40, Min: 2, Max: 20, Last: 20}
	cases := []struct {
		agg Aggregation
		v   float64
		ok  bool
	}{
		{AggLast, 20, true}, {AggAvg, 10, true}, {AggMin, 2, true},
		{AggMax, 20, true}, {AggSum, 40, true}, {AggCnt, 4, true},
		{Aggregation("bogus"), 0, false},
	}
	for _, c := range cases {
		got, ok := a.Value(c.agg)
		if ok != c.ok || math.Abs(got-c.v) > 1e-9 {
			t.Fatalf("Value(%s)=(%v,%v) want (%v,%v)", c.agg, got, ok, c.v, c.ok)
		}
	}
	empty := Aggregate{}
	if _, ok := empty.Value(AggAvg); ok {
		t.Fatal("avg of empty window must not be usable")
	}
}

func TestCompareAndViolating(t *testing.T) {
	cases := []struct {
		op   ComparisonOp
		want bool
	}{
		{OpGT, true}, {OpGE, true}, {OpLT, false}, {OpLE, false}, {OpEQ, false},
	}
	for _, c := range cases {
		if Compare(c.op, 10, 5) != c.want {
			t.Fatalf("Compare(10,5) with %s = %v want %v", c.op, !c.want, c.want)
		}
	}
	r := Rule{Agg: AggAvg, Op: OpGT, Threshold: 8}
	a := Aggregate{Count: 2, Sum: 20}
	if v, violating := r.Violating(a); !violating || v != 10 {
		t.Fatalf("Violating = (%v,%v) want (10,true)", v, violating)
	}
}

func TestAlertKeyAndCloneLabels(t *testing.T) {
	if got := AlertKey("r1", "cpu{host=h}"); got != "r1|cpu{host=h}" {
		t.Fatalf("alert key = %q", got)
	}
	src := map[string]string{"a": "1"}
	cp := CloneLabels(src)
	cp["a"] = "2"
	cp["b"] = "3"
	if src["a"] != "1" {
		t.Fatal("clone must not alias the source map")
	}
}
