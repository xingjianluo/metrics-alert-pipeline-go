// Package model defines the shared data model of the metrics alert pipeline:
// raw samples, windowed aggregates, alerting rules and alert events.
package model

import "time"

// Sample is one raw metric observation reported by a collector.
type Sample struct {
	Name      string
	Labels    map[string]string
	Value     float64
	Timestamp time.Time
}

// Aggregate is the aggregated result of one series over one window.
type Aggregate struct {
	Name        string
	Labels      map[string]string
	WindowStart time.Time
	WindowEnd   time.Time
	Count       int64
	Sum         float64
	Min         float64
	Max         float64
	Last        float64
}

// ComparisonOp is the predicate operator used by a rule.
type ComparisonOp string

const (
	OpGT ComparisonOp = ">"
	OpGE ComparisonOp = ">="
	OpLT ComparisonOp = "<"
	OpLE ComparisonOp = "<="
	OpEQ ComparisonOp = "=="
)

// Aggregation selects which aggregated field a rule compares against.
type Aggregation string

const (
	AggLast Aggregation = "last"
	AggAvg  Aggregation = "avg"
	AggMin  Aggregation = "min"
	AggMax  Aggregation = "max"
	AggSum  Aggregation = "sum"
	AggCnt  Aggregation = "count"
)

// Rule is an alerting rule definition.
type Rule struct {
	ID           string
	Metric       string
	MatchLabels  map[string]string
	Agg          Aggregation
	Op           ComparisonOp
	Threshold    float64
	Window       time.Duration
	ForDuration  time.Duration // must be continuously violating for this long before firing
	KeepDuration time.Duration // must be continuously recovering for this long before resolving
}

// AlertState is the state of one alert instance.
type AlertState string

const (
	StateInactive   AlertState = "inactive"
	StatePending    AlertState = "pending"
	StateFiring     AlertState = "firing"
	StateRecovering AlertState = "recovering"
)

// EventType classifies alert events.
type EventType string

const (
	EventPending    EventType = "pending"
	EventFired      EventType = "fired"
	EventRecovering EventType = "recovering"
	EventResolved   EventType = "resolved"
)

// Event is one deduplicated state-transition event of an alert instance.
type Event struct {
	RuleID     string
	Key        string // alert identity: rule id + label fingerprint
	Labels     map[string]string
	Type       EventType
	Value      float64
	OccurredAt time.Time
}
