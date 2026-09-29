// Package model defines the core data types shared by the ingestion,
// aggregation, alerting and pipeline layers.
package model

import "time"

// Sample is a single metric sample reported by a collector.
type Sample struct {
	Name      string
	Labels    map[string]string
	Value     float64
	Timestamp time.Time
}

// SeriesKey identifies a unique label combination for a metric name.
// It is rendered as name|k1=v1,k2=v2 with keys sorted, so equal label
// sets always map to the same key regardless of map iteration order.
type SeriesKey string

// WindowAggregate is the aggregated statistics of one series inside a
// single aggregation window. Count includes every accepted sample
// (duplicates are merged before aggregation, see store).
type WindowAggregate struct {
	Series      SeriesKey
	Name        string
	Labels      map[string]string
	WindowStart time.Time
	WindowEnd   time.Time
	Count       int64
	Sum         float64
	Min         float64
	Max         float64
	Latest      float64 // value of the sample with the greatest timestamp
}

// AlertState is the state of one alerting series for one rule.
type AlertState int

const (
	// StatePending means the threshold has been met but the firing
	// duration has not been sustained long enough yet.
	StatePending AlertState = iota
	// StateFiring means the alert is active.
	StateFiring
	// StateResolving means the condition cleared but the recovery
	// duration has not been sustained long enough yet.
	StateResolving
	// StateInactive means the alert is fully resolved / never fired.
	StateInactive
)

func (s AlertState) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateFiring:
		return "firing"
	case StateResolving:
		return "resolving"
	default:
		return "inactive"
	}
}

// AlertEvent is a state-transition result emitted by the engine.
type AlertEvent struct {
	RuleID     string
	Series     SeriesKey
	Name       string
	Labels     map[string]string
	State      AlertState
	Value      float64
	EvalTime   time.Time
	FiredAt    time.Time // zero unless State == StateFiring
	ResolvedAt time.Time // zero unless State == StateInactive after firing
}

// RejectReason explains why an ingest operation was refused.
type RejectReason int

const (
	RejectNone RejectReason = iota
	// RejectCardinality means a new series would exceed the configured
	// max series limit.
	RejectCardinality
	// RejectQueueFull means the ingest queue was full under the
	// configured overflow policy.
	RejectQueueFull
	// RejectStale means the sample arrived later than the allowed
	// out-of-order bound past its window end.
	RejectStale
	// RejectInvalid means the sample failed validation (empty name,
	// bad timestamp, ...).
	RejectInvalid
)

func (r RejectReason) String() string {
	switch r {
	case RejectCardinality:
		return "cardinality_limit_exceeded"
	case RejectQueueFull:
		return "queue_full"
	case RejectStale:
		return "stale_sample"
	case RejectInvalid:
		return "invalid_sample"
	default:
		return "none"
	}
}

// RejectError carries an explicit reason for a refused write.
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	return e.Reason.String() + ": " + e.Detail
}
