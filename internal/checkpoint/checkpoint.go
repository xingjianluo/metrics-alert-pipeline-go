// Package checkpoint persists the pipeline's recoverable state (alert
// state machine, unflushed window data, recent event history) to local
// disk and restores it after a restart.
//
// Reliability model:
//   - every checkpoint is written to a temp file and atomically renamed;
//   - each file carries a CRC32 checksum of its payload;
//   - the last MaxCheckpoints files are retained; loading walks
//     newest -> oldest and uses the first file that parses and checks
//     out, so a half-written or corrupted newest file falls back to the
//     previous good one;
//   - if NO retained file is usable, Load reports a hard error instead
//     of silently starting empty (which would cause false/missed alerts);
//   - the payload is format-versioned: unknown or newer versions are
//     rejected with an explicit reason; older versions go through the
//     migration table (currently only v1 exists, so anything else is a
//     clear refusal, never a silent empty start).
package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/engine"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/store"
)

// FormatVersion is the current on-disk payload version.
const FormatVersion = 1

// ErrNoCheckpoint marks the clean-start case: the directory holds no
// checkpoint files at all. Callers treat it as "start empty".
var ErrNoCheckpoint = errors.New("checkpoint: no checkpoint found")

// Payload is the versioned, checksummed unit persisted per checkpoint.
type Payload struct {
	FormatVersion int                     `json:"format_version"`
	WrittenAt     time.Time               `json:"written_at"`
	RuleVersion   int                     `json:"rule_version"`
	Rules         []rule.Rule             `json:"rules"`
	Alerts        []engine.PersistedAlert `json:"alerts"`
	Windows       store.PersistedStore    `json:"windows"`
	Events        []model.AlertEvent      `json:"events,omitempty"`
}

// envelope wraps the payload with its checksum.
type envelope struct {
	Checksum uint32          `json:"checksum"`
	Payload  json.RawMessage `json:"payload"`
}

// Options configures a Manager. Zero fields fall back to defaults.
type Options struct {
	// Dir holds checkpoint files. Required.
	Dir string
	// MaxCheckpoints bounds how many checkpoint files are retained
	// (default 3). Older files are pruned after each successful save.
	MaxCheckpoints int
	// MaxEvents bounds the event history persisted per checkpoint
	// (default 256); the newest events are kept.
	MaxEvents int
}

func (o *Options) withDefaults() {
	if o.MaxCheckpoints <= 0 {
		o.MaxCheckpoints = 3
	}
	if o.MaxEvents <= 0 {
		o.MaxEvents = 256
	}
}

// Manager saves and loads checkpoints under one directory. Save is
// serialized internally, so concurrent callers (periodic ticker and
// final shutdown save) cannot interleave a file.
type Manager struct {
	opt Options
	mu  sync.Mutex // serializes Save
}

// NewManager validates options and creates the directory if needed.
func NewManager(opt Options) (*Manager, error) {
	if opt.Dir == "" {
		return nil, errors.New("checkpoint: Dir is required")
	}
	opt.withDefaults()
	if err := os.MkdirAll(opt.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint: create dir: %w", err)
	}
	return &Manager{opt: opt}, nil
}

// Options returns the resolved options.
func (m *Manager) Options() Options { return m.opt }

// Save persists one checkpoint atomically (temp file + rename) and
// prunes files beyond the retention limit.
func (m *Manager) Save(p Payload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	p.FormatVersion = FormatVersion
	if p.WrittenAt.IsZero() {
		p.WrittenAt = time.Now()
	}
	// Bound the persisted event history: keep the newest MaxEvents.
	if len(p.Events) > m.opt.MaxEvents {
		p.Events = p.Events[len(p.Events)-m.opt.MaxEvents:]
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("checkpoint: encode payload: %w", err)
	}
	env := envelope{Checksum: crc32.ChecksumIEEE(raw), Payload: raw}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("checkpoint: encode envelope: %w", err)
	}

	name := fileName(p.WrittenAt)
	tmp := filepath.Join(m.opt.Dir, name+".tmp")
	final := filepath.Join(m.opt.Dir, name)
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("checkpoint: write temp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	return m.prune()
}

// Load returns the newest usable checkpoint. Corrupt, half-written or
// version-incompatible files are skipped in favour of older ones (each
// skip is reported via the returned SkipLog). A non-empty directory
// with no usable file is a hard error: starting empty would silently
// re-fire or drop alerts. ErrNoCheckpoint means a clean first start.
func (m *Manager) Load() (Payload, error) {
	p, _, err := m.LoadWithLog()
	return p, err
}

// LoadWithLog is Load plus the human-readable skip reasons, so the
// caller can log exactly why a file was not used.
func (m *Manager) LoadWithLog() (Payload, []string, error) {
	files, err := m.list()
	if err != nil {
		return Payload{}, nil, err
	}
	if len(files) == 0 {
		return Payload{}, nil, ErrNoCheckpoint
	}
	var skips []string
	for _, f := range files { // newest first
		p, err := m.readOne(f)
		if err != nil {
			skips = append(skips, filepath.Base(f)+": "+err.Error())
			continue
		}
		return p, skips, nil
	}
	return Payload{}, skips, fmt.Errorf(
		"checkpoint: %d file(s) found but none usable; refusing to start empty: %s",
		len(files), strings.Join(skips, "; "))
}

// readOne parses, verifies and version-checks one file.
func (m *Manager) readOne(path string) (Payload, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Payload{}, err
	}
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Payload{}, fmt.Errorf("unparseable (corrupt or half-written): %v", err)
	}
	if crc32.ChecksumIEEE(env.Payload) != env.Checksum {
		return Payload{}, errors.New("checksum mismatch (corrupt or half-written)")
	}
	var head struct {
		FormatVersion int `json:"format_version"`
	}
	if err := json.Unmarshal(env.Payload, &head); err != nil {
		return Payload{}, fmt.Errorf("unparseable payload: %v", err)
	}
	if err := checkVersion(head.FormatVersion); err != nil {
		return Payload{}, err
	}
	var p Payload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return Payload{}, fmt.Errorf("decode payload: %v", err)
	}
	return p, nil
}

// checkVersion is the migration/refusal point. Only FormatVersion is
// understood today; anything else is an explicit refusal with the
// reason, never a silent empty start.
func checkVersion(v int) error {
	switch {
	case v == FormatVersion:
		return nil
	case v < 1:
		return fmt.Errorf("unsupported format version %d (corrupt header?)", v)
	case v < FormatVersion:
		// Migration hook for future versions: v1 is the first, so no
		// older format can exist yet. Keep the branch to make the
		// migration point explicit.
		return fmt.Errorf("format version %d is older than %d and has no migration", v, FormatVersion)
	default:
		return fmt.Errorf("format version %d is newer than supported %d (upgrade the binary or remove the checkpoint)", v, FormatVersion)
	}
}

// fileName sorts lexicographically by time then is unique per save.
func fileName(t time.Time) string {
	return "ckpt-" + t.UTC().Format("20060102T150405.000000000") + ".json"
}

// list returns checkpoint files newest first.
func (m *Manager) list() ([]string, error) {
	ents, err := os.ReadDir(m.opt.Dir)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read dir: %w", err)
	}
	var names []string
	for _, e := range ents {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "ckpt-") ||
			!strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		names = append(names, e.Name())
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, filepath.Join(m.opt.Dir, n))
	}
	return out, nil
}

// prune keeps only the newest MaxCheckpoints files.
func (m *Manager) prune() error {
	files, err := m.list()
	if err != nil {
		return err
	}
	if len(files) <= m.opt.MaxCheckpoints {
		return nil
	}
	for _, f := range files[m.opt.MaxCheckpoints:] {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("checkpoint: prune %s: %w", filepath.Base(f), err)
		}
	}
	return nil
}

// DirSize reports the total bytes held by checkpoint files (observability
// for the space bound).
func (m *Manager) DirSize() (int64, error) {
	files, err := m.list()
	if err != nil {
		return 0, err
	}
	var total int64
	for _, f := range files {
		if fi, err := os.Stat(f); err == nil {
			total += fi.Size()
		}
	}
	return total, nil
}
