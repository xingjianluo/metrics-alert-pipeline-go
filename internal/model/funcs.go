package model

import (
	"fmt"
	"sort"
	"strings"
)

// Fingerprint returns a canonical identity for a metric name + label set.
// Label sets with the same key/value pairs always produce the same string,
// regardless of map iteration order.
func Fingerprint(name string, labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	b.WriteByte('}')
	return b.String()
}

// MatchLabels reports whether labels contain every required key/value pair.
// An empty requirement set matches every series.
func MatchLabels(labels, required map[string]string) bool {
	for k, v := range required {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// CloneLabels returns a copy of labels so callers can retain it safely.
func CloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

// Value returns the aggregated field selected by the rule.
func (a Aggregate) Value(agg Aggregation) (float64, bool) {
	switch agg {
	case AggLast:
		return a.Last, true
	case AggAvg:
		if a.Count == 0 {
			return 0, false
		}
		return a.Sum / float64(a.Count), true
	case AggMin:
		return a.Min, a.Count > 0
	case AggMax:
		return a.Max, a.Count > 0
	case AggSum:
		return a.Sum, true
	case AggCnt:
		return float64(a.Count), true
	default:
		return 0, false
	}
}

// Compare evaluates op(left, threshold).
func Compare(op ComparisonOp, left, right float64) bool {
	switch op {
	case OpGT:
		return left > right
	case OpGE:
		return left >= right
	case OpLT:
		return left < right
	case OpLE:
		return left <= right
	case OpEQ:
		return left == right
	default:
		return false
	}
}

// Violating reports whether the aggregate violates the rule predicate.
func (r Rule) Violating(a Aggregate) (float64, bool) {
	v, ok := a.Value(r.Agg)
	if !ok {
		return 0, false
	}
	return v, Compare(r.Op, v, r.Threshold)
}

// AlertKey is the identity of an alert instance: rule + series fingerprint.
func AlertKey(ruleID, seriesKey string) string {
	return fmt.Sprintf("%s|%s", ruleID, seriesKey)
}
