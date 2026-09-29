// Package rule defines alerting rules and the threshold predicate
// evaluated against window aggregates.
package rule

import (
	"errors"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// Operator is the comparison applied to the chosen aggregate value.
type Operator string

const (
	OpGT Operator = "gt"
	OpGE Operator = "ge"
	OpLT Operator = "lt"
	OpLE Operator = "le"
	OpEQ Operator = "eq"
	OpNE Operator = "ne"
)

// ValueField selects which part of a WindowAggregate is compared.
type ValueField string

const (
	FieldAvg    ValueField = "avg"
	FieldLatest ValueField = "latest"
	FieldSum    ValueField = "sum"
	FieldMin    ValueField = "min"
	FieldMax    ValueField = "max"
)

// Rule expresses: when ValueField of Metric's window aggregate
// (optionally label-filtered) satisfies Operator Threshold continuously
// for ForDuration, the alert fires; once it stops satisfying the
// condition continuously for KeepResolvedFor, it resolves.
type Rule struct {
	ID              string
	Metric          string
	LabelFilter     map[string]string // empty/nil matches every series
	Field           ValueField
	Op              Operator
	Threshold       float64
	ForDuration     time.Duration
	KeepResolvedFor time.Duration
}

// Validate checks a rule definition.
func (r Rule) Validate() error {
	if r.ID == "" {
		return errors.New("rule: missing id")
	}
	if r.Metric == "" {
		return errors.New("rule " + r.ID + ": missing metric")
	}
	switch r.Op {
	case OpGT, OpGE, OpLT, OpLE, OpEQ, OpNE:
	default:
		return errors.New("rule " + r.ID + ": unsupported operator " + string(r.Op))
	}
	switch r.Field {
	case "", FieldAvg, FieldLatest, FieldSum, FieldMin, FieldMax:
	default:
		return errors.New("rule " + r.ID + ": unsupported value field " + string(r.Field))
	}
	if r.ForDuration < 0 || r.KeepResolvedFor < 0 {
		return errors.New("rule " + r.ID + ": durations must not be negative")
	}
	return nil
}

// Matches reports whether the aggregate belongs to this rule's series
// and satisfies the threshold condition.
func (r Rule) Matches(agg model.WindowAggregate) bool {
	if agg.Name != r.Metric {
		return false
	}
	for k, v := range r.LabelFilter {
		if agg.Labels[k] != v {
			return false
		}
	}
	return r.compare(extract(agg, r.Field))
}

func extract(agg model.WindowAggregate, f ValueField) float64 {
	switch f {
	case FieldSum:
		return agg.Sum
	case FieldMin:
		return agg.Min
	case FieldMax:
		return agg.Max
	case FieldLatest:
		return agg.Latest
	default: // FieldAvg / ""
		if agg.Count == 0 {
			return 0
		}
		return agg.Sum / float64(agg.Count)
	}
}

func (r Rule) compare(v float64) bool {
	switch r.Op {
	case OpGT:
		return v > r.Threshold
	case OpGE:
		return v >= r.Threshold
	case OpLT:
		return v < r.Threshold
	case OpLE:
		return v <= r.Threshold
	case OpEQ:
		return v == r.Threshold
	case OpNE:
		return v != r.Threshold
	default:
		return false
	}
}

// Snapshot is an immutable, versioned set of rules.
type Snapshot struct {
	Version int
	Rules   []Rule
}

// Validate checks every rule and rejects duplicate IDs.
func (snap Snapshot) Validate() error {
	seen := make(map[string]struct{}, len(snap.Rules))
	for _, r := range snap.Rules {
		if err := r.Validate(); err != nil {
			return err
		}
		if _, dup := seen[r.ID]; dup {
			return errors.New("rule: duplicate id " + r.ID)
		}
		seen[r.ID] = struct{}{}
	}
	return nil
}

// ByID indexes rules for lookup.
func (snap Snapshot) ByID() map[string]Rule {
	m := make(map[string]Rule, len(snap.Rules))
	for _, r := range snap.Rules {
		m[r.ID] = r
	}
	return m
}
