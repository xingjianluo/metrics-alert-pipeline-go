// Package checkpoint persists pipeline state (alert states, unflushed
// window data, recent event history) to local disk and restores it on
// process restart, so evaluation continues instead of starting cold.
//
// On-disk layout: one JSON file per checkpoint inside a directory,
// named checkpoint-<seq>.json. Writes are atomic (tmp file + fsync +
// rename), so a crash mid-write never leaves a half-written checkpoint
// behind — at worst the tmp file is orphaned and ignored. Loading scans
// newest to oldest and falls back to the first usable file; a corrupt
// newest file never silently becomes an empty state. Old checkpoints
// beyond the retention cap are pruned, bounding disk usage.
//
// Versioning: the envelope carries a format version. A checkpoint
// written by a newer binary is refused with an explicit error; an
// older known version is migrated by registered migrators (none are
// needed while FormatVersion == 1; unknown versions are refused with a
// clear reason rather than silently treated as empty).
package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

// FormatVersion is the current on-disk format version.
const FormatVersion = 1

// DefaultKeep is the default number of retained checkpoints.
const DefaultKeep = 3

var (
	// ErrNoCheckpoint means the directory holds no checkpoint file at
	// all: a clean first boot, not an error.
	ErrNoCheckpoint = errors.New("checkpoint: no checkpoint found")
	// ErrNoUsableCheckpoint means checkpoint files exist but every one
	// is corrupt or truncated. Starting empty would silently lose
	// in-flight alerts, so callers must fail loudly.
	ErrNoUsableCheckpoint = errors.New("checkpoint: no usable checkpoint")
	// ErrUnsupportedVersion means the checkpoint format is newer than
	// (or otherwise unknown to) this binary.
	ErrUnsupportedVersion = errors.New("checkpoint: unsupported format version")
)

// AlertRecord is the restorable state of one (rule, series) alert.
type AlertRecord struct {
	RuleID  string
	Series  string
	State   string // pending|firing|resolving|inactive
	Since   time.Time
	FiredAt time.Time
	Episode bool
}

// WindowRecord is one not-yet-flushed aggregation window of a series.
type WindowRecord struct {
	Series string
	Name   string
	Labels map[string]string
	Start  time.Time
	Values map[time.Time]float64
}

// State is the full restorable payload of one checkpoint.
type State struct {
	SavedAt      time.Time
	RuleVersion  int
	LatestWindow time.Time
	Alerts       []AlertRecord
	Windows      []WindowRecord
	Events       []model.AlertEvent // bounded recent history
}

// LoadResult describes a Load outcome for explainable logging.
type LoadResult struct {
	State      *State
	File       string   // file the state was loaded from
	SkippedBad []string // newer files skipped as corrupt/incompatible
}

// envelope is the on-disk wrapper: format version + CRC32 of the
// payload, so truncation and torn content are detected explicitly
// instead of relying on JSON parse failure alone.
type envelope struct {
	Format   int             `json:"format"`
	Checksum uint32          `json:"checksum"`
	Payload  json.RawMessage `json:"payload"`
}

// Manager owns a checkpoint directory: atomic saves, retained-count
// pruning and fallback loading.
type Manager struct {
	dir  string
	keep int
	log  *slog.Logger

	mu  sync.Mutex // serializes Save
	seq int        // last used sequence number
}

// NewManager opens (creating if needed) a checkpoint directory. keep is
// the number of checkpoints retained; <= 0 falls back to DefaultKeep.
// log may be nil (slog.Default is used).
func NewManager(dir string, keep int, log *slog.Logger) (*Manager, error) {
	if dir == "" {
		return nil, errors.New("checkpoint: empty directory")
	}
	if keep <= 0 {
		keep = DefaultKeep
	}
	if log == nil {
		log = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint: create dir: %w", err)
	}
	m := &Manager{dir: dir, keep: keep, log: log}
	// Continue the sequence after the highest existing checkpoint so
	// file names stay monotonic across restarts.
	for _, f := range listCheckpoints(dir) {
		if n, ok := seqOf(f); ok && n > m.seq {
			m.seq = n
		}
	}
	return m, nil
}

// Dir returns the managed directory (for tests and logging).
func (m *Manager) Dir() string { return m.dir }

// Save atomically persists s and prunes old checkpoints beyond the
// retention cap. It is safe for concurrent callers (serialized).
func (m *Manager) Save(s State) error {
	payload, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal state: %w", err)
	}
	env := envelope{
		Format:   FormatVersion,
		Checksum: crc32.ChecksumIEEE(payload),
		Payload:  payload,
	}
	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal envelope: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.seq++
	name := fmt.Sprintf("checkpoint-%06d.json", m.seq)
	final := filepath.Join(m.dir, name)
	tmp := filepath.Join(m.dir, "."+name+".tmp")

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("checkpoint: create tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: fsync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: close tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	m.pruneLocked()
	return nil
}

// pruneLocked keeps only the newest keep checkpoints. Caller holds mu.
func (m *Manager) pruneLocked() {
	files := listCheckpoints(m.dir)
	if len(files) <= m.keep {
		return
	}
	// listCheckpoints is sorted oldest first.
	for _, f := range files[:len(files)-m.keep] {
		if err := os.Remove(filepath.Join(m.dir, f)); err != nil {
			m.log.Warn("checkpoint: prune failed", "file", f, "err", err)
		}
	}
}

// Load returns the newest usable checkpoint. Newer corrupt or
// incompatible files are skipped and reported in the result; if files
// exist but none is usable the error is ErrNoUsableCheckpoint (never a
// silent empty state). An empty directory yields ErrNoCheckpoint.
func (m *Manager) Load() (LoadResult, error) {
	files := listCheckpoints(m.dir)
	if len(files) == 0 {
		return LoadResult{}, ErrNoCheckpoint
	}
	res := LoadResult{}
	versionBad := false
	// Newest first: fall back to older checkpoints on corruption.
	for i := len(files) - 1; i >= 0; i-- {
		st, err := loadFile(filepath.Join(m.dir, files[i]))
		if err != nil {
			if errors.Is(err, ErrUnsupportedVersion) {
				versionBad = true
			}
			res.SkippedBad = append(res.SkippedBad, files[i]+": "+err.Error())
			continue
		}
		res.State = st
		res.File = files[i]
		return res, nil
	}
	if len(res.SkippedBad) > 0 {
		detail := fmt.Sprintf("%d file(s) unreadable: %s",
			len(res.SkippedBad), strings.Join(res.SkippedBad, "; "))
		if versionBad {
			// Every file failed and at least one was a version
			// mismatch: surface both facts so the caller can say
			// "upgrade the binary" instead of just "corrupt".
			return res, fmt.Errorf("%w: %w (%s)",
				ErrNoUsableCheckpoint, ErrUnsupportedVersion, detail)
		}
		return res, fmt.Errorf("%w (%s)", ErrNoUsableCheckpoint, detail)
	}
	return res, ErrNoCheckpoint
}

// loadFile reads and validates one checkpoint file.
func loadFile(path string) (*State, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unparseable envelope: %w", err)
	}
	if env.Format > FormatVersion {
		return nil, fmt.Errorf("%w: file has %d, binary supports %d",
			ErrUnsupportedVersion, env.Format, FormatVersion)
	}
	if env.Format < 1 {
		return nil, fmt.Errorf("%w: file has %d, no migration path",
			ErrUnsupportedVersion, env.Format)
	}
	if crc32.ChecksumIEEE(env.Payload) != env.Checksum {
		return nil, errors.New("checksum mismatch (truncated or corrupt)")
	}
	var st State
	if err := json.Unmarshal(env.Payload, &st); err != nil {
		return nil, fmt.Errorf("unparseable payload: %w", err)
	}
	return &st, nil
}

// listCheckpoints returns checkpoint file names in dir, sorted by
// sequence (oldest first). Tmp/orphan files are ignored.
func listCheckpoints(dir string) []string {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		if _, ok := seqOf(e.Name()); ok {
			names = append(names, e.Name())
		}
	}
	sort.Slice(names, func(i, j int) bool {
		a, _ := seqOf(names[i])
		b, _ := seqOf(names[j])
		return a < b
	})
	return names
}

// seqOf extracts the sequence number from a checkpoint file name.
func seqOf(name string) (int, bool) {
	if !strings.HasPrefix(name, "checkpoint-") || !strings.HasSuffix(name, ".json") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(name, "checkpoint-"), ".json"))
	if err != nil {
		return 0, false
	}
	return n, true
}
