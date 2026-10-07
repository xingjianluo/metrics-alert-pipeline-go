package pipeline

import (
	"errors"
	"fmt"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/checkpoint"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// restore loads the newest usable checkpoint and installs it into the
// engine and store. Rule compatibility follows hot-update semantics:
// states of rules still present continue, states of deleted rules are
// dropped (and logged), new rules start clean. A corrupt newest
// checkpoint falls back to an older usable one; existing-but-unusable
// checkpoints abort startup with an explicit reason instead of silently
// running on empty state.
func (p *Pipeline) restore() error {
	res, err := p.ckpt.Load()
	if err != nil {
		if errors.Is(err, checkpoint.ErrNoCheckpoint) {
			p.log.Info("no checkpoint found, starting with clean state",
				"dir", p.ckpt.Dir())
			return nil
		}
		// ErrNoUsableCheckpoint / ErrUnsupportedVersion: refuse to
		// start, the reason is in the error text.
		return fmt.Errorf("pipeline: refusing to start: %w", err)
	}
	for _, skipped := range res.SkippedBad {
		p.log.Warn("checkpoint skipped as unusable, fell back",
			"file", skipped)
	}
	st := res.State

	rep := p.engine.Restore(st.Alerts)
	p.store.Restore(st.Windows, st.LatestWindow)

	p.mu.Lock()
	p.events = append(p.events, st.Events...)
	p.trimEventsLocked()
	p.mu.Unlock()

	p.log.Info("checkpoint restored",
		"file", res.File,
		"saved_at", st.SavedAt,
		"rule_version_then", st.RuleVersion,
		"rule_version_now", p.cfg.Ruleset.Version,
		"alerts_restored", rep.Restored,
		"alerts_dropped_rules", rep.DroppedRule,
		"windows_restored", len(st.Windows),
		"events_restored", len(st.Events))
	return nil
}

// saveCheckpoint snapshots engine state, unflushed windows and the
// bounded event history, then persists them atomically. Snapshots are
// taken under the component locks but the file write itself holds no
// pipeline lock, so ingestion and evaluation are not blocked by disk
// I/O. Failures are logged, never fatal: the next tick retries.
func (p *Pipeline) saveCheckpoint() {
	if p.ckpt == nil {
		return
	}
	wins, latest := p.store.Export()
	p.mu.Lock()
	events := make([]model.AlertEvent, len(p.events))
	copy(events, p.events)
	p.mu.Unlock()
	st := checkpoint.State{
		SavedAt:      p.now(),
		RuleVersion:  p.engine.Rules().Version,
		LatestWindow: latest,
		Alerts:       p.engine.Export(),
		Windows:      wins,
		Events:       events,
	}
	if err := p.ckpt.Save(st); err != nil {
		p.log.Error("checkpoint save failed", "err", err)
		return
	}
	p.log.Info("checkpoint saved",
		"alerts", len(st.Alerts), "windows", len(st.Windows),
		"events", len(st.Events))
}

// recordEventLocked appends ev to the bounded recent-event history.
// Caller holds p.mu.
func (p *Pipeline) recordEventLocked(ev model.AlertEvent) {
	p.events = append(p.events, ev)
	p.trimEventsLocked()
}

// trimEventsLocked enforces the event-history retention cap, dropping
// the oldest events first. Caller holds p.mu.
func (p *Pipeline) trimEventsLocked() {
	if len(p.events) > p.cfg.MaxEventsKept {
		p.events = append([]model.AlertEvent(nil),
			p.events[len(p.events)-p.cfg.MaxEventsKept:]...)
	}
}

// RecentEvents returns a copy of the bounded recent-event history
// (oldest first). Restored history survives restarts.
func (p *Pipeline) RecentEvents() []model.AlertEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]model.AlertEvent, len(p.events))
	copy(out, p.events)
	return out
}
