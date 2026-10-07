package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func sampleState(seq int) State {
	return State{
		SavedAt:      time.Date(2026, 1, 1, 0, 0, seq, 0, time.UTC),
		RuleVersion:  seq,
		LatestWindow: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Alerts: []AlertRecord{{
			RuleID: "high_cpu", Series: "cpu_usage|host=a",
			State: "firing", Episode: true,
			Since:   time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
			FiredAt: time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC),
		}},
		Windows: []WindowRecord{{
			Series: "cpu_usage|host=a", Name: "cpu_usage",
			Labels: map[string]string{"host": "a"},
			Start:  time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC),
			Values: map[time.Time]float64{
				time.Date(2026, 1, 1, 0, 0, 3, 100e6, time.UTC): 0.9,
			},
		}},
	}
}

func TestSaveLoadRoundtrip(t *testing.T) {
	t.Log("场景: 正常落盘后加载, 状态逐字段一致")
	m, err := NewManager(t.TempDir(), 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := sampleState(1)
	if err := m.Save(want); err != nil {
		t.Fatal(err)
	}
	res, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	got := res.State
	if got.RuleVersion != want.RuleVersion || !got.SavedAt.Equal(want.SavedAt) {
		t.Fatalf("header mismatch: %+v", got)
	}
	if len(got.Alerts) != 1 || got.Alerts[0] != want.Alerts[0] {
		t.Fatalf("alerts mismatch: %+v", got.Alerts)
	}
	if len(got.Windows) != 1 || got.Windows[0].Values[time.Date(2026, 1, 1, 0, 0, 3, 100e6, time.UTC)] != 0.9 {
		t.Fatalf("windows mismatch: %+v", got.Windows)
	}
}

func TestLoadEmptyDirIsCleanBoot(t *testing.T) {
	t.Log("场景: 空目录 = 首次启动, 返回 ErrNoCheckpoint 而非失败")
	m, _ := NewManager(t.TempDir(), 3, nil)
	_, err := m.Load()
	if !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("want ErrNoCheckpoint, got %v", err)
	}
}

func TestCorruptNewestFallsBackToPrevious(t *testing.T) {
	t.Log("场景: 最新检查点损坏(截断), 自动回退到上一份可用检查点")
	dir := t.TempDir()
	m, _ := NewManager(dir, 5, nil)
	if err := m.Save(sampleState(1)); err != nil {
		t.Fatal(err)
	}
	if err := m.Save(sampleState(2)); err != nil {
		t.Fatal(err)
	}
	// Corrupt the newest file: truncate it mid-payload.
	files, _ := filepath.Glob(filepath.Join(dir, "checkpoint-*.json"))
	var newest string
	for _, f := range files {
		if f > newest {
			newest = f
		}
	}
	fi, _ := os.Stat(newest)
	if err := os.Truncate(newest, fi.Size()/2); err != nil {
		t.Fatal(err)
	}

	res, err := m.Load()
	if err != nil {
		t.Fatalf("expected fallback to succeed, got %v", err)
	}
	if res.State.RuleVersion != 1 {
		t.Fatalf("expected state from checkpoint 1, got rule version %d", res.State.RuleVersion)
	}
	if len(res.SkippedBad) != 1 {
		t.Fatalf("expected 1 skipped corrupt file, got %v", res.SkippedBad)
	}
	t.Logf("回退成功: 加载 %s, 跳过损坏文件 %s", res.File, res.SkippedBad[0])
}

func TestAllCorruptRefusesToStart(t *testing.T) {
	t.Log("场景: 目录里只有损坏文件, 明确拒绝(ErrNoUsableCheckpoint), 不静默按空状态启动")
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "checkpoint-000001.json"), []byte("{garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	m, _ := NewManager(dir, 3, nil)
	_, err := m.Load()
	if !errors.Is(err, ErrNoUsableCheckpoint) {
		t.Fatalf("want ErrNoUsableCheckpoint, got %v", err)
	}
	t.Logf("拒绝原因: %v", err)
}

func TestNewerVersionRefused(t *testing.T) {
	t.Log("场景: 检查点格式比当前二进制新, 明确拒绝并说明版本, 不按空状态继续")
	dir := t.TempDir()
	m, _ := NewManager(dir, 3, nil)
	if err := m.Save(sampleState(1)); err != nil {
		t.Fatal(err)
	}
	// Rewrite the only checkpoint with a bumped format version.
	files, _ := filepath.Glob(filepath.Join(dir, "checkpoint-*.json"))
	data, _ := os.ReadFile(files[0])
	data = []byte(strings.Replace(string(data), `"format":1`, `"format":99`, 1))
	if err := os.WriteFile(files[0], data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := m.Load()
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("want ErrUnsupportedVersion, got %v", err)
	}
	if !strings.Contains(err.Error(), "99") {
		t.Fatalf("error should name the unsupported version: %v", err)
	}
	t.Logf("拒绝原因: %v", err)
}

func TestRetentionPrunesOldCheckpoints(t *testing.T) {
	t.Log("场景: 保留份数有上限, 超出后最旧的检查点被清理, 空间不无限增长")
	dir := t.TempDir()
	m, _ := NewManager(dir, 2, nil)
	for i := 0; i < 5; i++ {
		if err := m.Save(sampleState(i)); err != nil {
			t.Fatal(err)
		}
	}
	files, _ := filepath.Glob(filepath.Join(dir, "checkpoint-*.json"))
	if len(files) != 2 {
		t.Fatalf("want 2 retained checkpoints, got %d: %v", len(files), files)
	}
	res, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if res.State.RuleVersion != 4 {
		t.Fatalf("newest checkpoint should be the last save, got %d", res.State.RuleVersion)
	}
}

func TestOrphanTmpFileIgnored(t *testing.T) {
	t.Log("场景: 崩溃残留的 .tmp 半成品被忽略, 不影响加载上一份完整检查点")
	dir := t.TempDir()
	m, _ := NewManager(dir, 3, nil)
	if err := m.Save(sampleState(1)); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-write: an orphan tmp file with junk.
	if err := os.WriteFile(filepath.Join(dir, ".checkpoint-000002.json.tmp"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	res, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if res.State.RuleVersion != 1 {
		t.Fatalf("want state 1, got %d", res.State.RuleVersion)
	}
}

func TestSequenceContinuesAcrossManagers(t *testing.T) {
	t.Log("场景: 重启后序号延续, 新检查点不会覆盖旧文件命名")
	dir := t.TempDir()
	m1, _ := NewManager(dir, 10, nil)
	m1.Save(sampleState(1))
	m1.Save(sampleState(2))
	m2, _ := NewManager(dir, 10, nil)
	m2.Save(sampleState(3))
	files, _ := filepath.Glob(filepath.Join(dir, "checkpoint-*.json"))
	if len(files) != 3 {
		t.Fatalf("want 3 distinct checkpoint files, got %v", files)
	}
}
