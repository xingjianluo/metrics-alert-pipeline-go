package checkpoint

import (
	"encoding/json"
	"errors"
	"hash/crc32"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/engine"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/store"
)

func samplePayload(at time.Time) Payload {
	return Payload{
		WrittenAt:   at,
		RuleVersion: 7,
		Rules: []rule.Rule{{
			ID: "high_cpu", Metric: "cpu", Field: rule.FieldAvg, Op: rule.OpGT,
			Threshold: 0.8, ForDuration: 2 * time.Second, KeepResolvedFor: time.Second,
		}},
		Alerts: []engine.PersistedAlert{{
			RuleID: "high_cpu", Series: "cpu|host=a", State: model.StateFiring,
			Since: at, FiredAt: at, Episode: true,
		}},
		Windows: store.PersistedStore{
			LatestWindow: at.Truncate(time.Second),
			Series: []store.PersistedSeries{{
				Key: "cpu|host=a", Name: "cpu", Labels: map[string]string{"host": "a"},
				Windows: []store.PersistedWindow{{
					Start:  at.Truncate(time.Second),
					Values: map[time.Time]float64{at: 0.9},
				}},
			}},
		},
		Events: []model.AlertEvent{{
			RuleID: "high_cpu", Series: "cpu|host=a", State: model.StateFiring,
			EvalTime: at, FiredAt: at,
		}},
	}
}

func newMgr(t *testing.T, maxKeep, maxEvents int) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	m, err := NewManager(Options{Dir: dir, MaxCheckpoints: maxKeep, MaxEvents: maxEvents})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m, dir
}

func TestSaveLoadRoundtrip(t *testing.T) {
	t.Log("场景: 正常落盘后完整读回（状态机/窗口/事件历史/规则集）")
	m, _ := newMgr(t, 3, 10)
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	want := samplePayload(at)
	if err := m.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, err := m.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.FormatVersion != FormatVersion {
		t.Fatalf("format version = %d, want %d", got.FormatVersion, FormatVersion)
	}
	if got.RuleVersion != 7 || len(got.Rules) != 1 || got.Rules[0].ID != "high_cpu" {
		t.Fatalf("ruleset mismatch: %+v", got.Rules)
	}
	if len(got.Alerts) != 1 || !got.Alerts[0].Episode || got.Alerts[0].State != model.StateFiring {
		t.Fatalf("alerts mismatch: %+v", got.Alerts)
	}
	if len(got.Windows.Series) != 1 || len(got.Windows.Series[0].Windows) != 1 ||
		got.Windows.Series[0].Windows[0].Values[at] != 0.9 {
		t.Fatalf("windows mismatch: %+v", got.Windows)
	}
	if len(got.Events) != 1 || got.Events[0].State != model.StateFiring {
		t.Fatalf("events mismatch: %+v", got.Events)
	}
}

func TestLoadEmptyDirIsCleanStart(t *testing.T) {
	t.Log("场景: 首次启动（目录为空）返回 ErrNoCheckpoint，按干净状态起步")
	m, _ := newMgr(t, 0, 0)
	_, err := m.Load()
	if !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("err = %v, want ErrNoCheckpoint", err)
	}
}

func TestCorruptNewestFallsBackToPrevious(t *testing.T) {
	t.Log("场景: 最新检查点损坏 -> 自动回退到上一个可用检查点")
	m, _ := newMgr(t, 3, 10)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := m.Save(samplePayload(base)); err != nil {
		t.Fatal(err)
	}
	if err := m.Save(samplePayload(base.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	// Corrupt the newest file (flip bytes, checksum now mismatches).
	files, _ := m.list()
	data, _ := os.ReadFile(files[0])
	for i := range data[:len(data)/2] {
		data[i] ^= 0xFF
	}
	if err := os.WriteFile(files[0], data, 0o644); err != nil {
		t.Fatal(err)
	}
	got, skips, err := m.LoadWithLog()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(skips) != 1 {
		t.Fatalf("skips = %v, want 1 entry", skips)
	}
	t.Logf("跳过损坏文件原因: %s", skips[0])
	if !got.WrittenAt.Equal(base) {
		t.Fatalf("fell back to %v, want %v", got.WrittenAt, base)
	}
}

func TestHalfWrittenNewestFallsBack(t *testing.T) {
	t.Log("场景: 最新检查点只写了一半（截断）-> 回退到上一份，不当作空状态")
	m, _ := newMgr(t, 3, 10)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := m.Save(samplePayload(base)); err != nil {
		t.Fatal(err)
	}
	if err := m.Save(samplePayload(base.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	files, _ := m.list()
	data, _ := os.ReadFile(files[0])
	if err := os.WriteFile(files[0], data[:len(data)/3], 0o644); err != nil {
		t.Fatal(err)
	}
	got, _, err := m.LoadWithLog()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !got.WrittenAt.Equal(base) {
		t.Fatalf("fell back to %v, want %v", got.WrittenAt, base)
	}
}

func TestAllCorruptIsHardError(t *testing.T) {
	t.Log("场景: 所有留存检查点都损坏 -> 明确拒绝启动并说明原因，不悄悄按空状态跑")
	m, _ := newMgr(t, 3, 10)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := m.Save(samplePayload(base)); err != nil {
		t.Fatal(err)
	}
	files, _ := m.list()
	for _, f := range files {
		if err := os.WriteFile(f, []byte("{garbage"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	_, err := m.Load()
	if err == nil {
		t.Fatal("want hard error, got nil")
	}
	if errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("must not be reported as clean start: %v", err)
	}
	if !strings.Contains(err.Error(), "refusing to start empty") {
		t.Fatalf("error should explain refusal: %v", err)
	}
	t.Logf("拒绝启动原因: %v", err)
}

func TestNewerFormatVersionRefused(t *testing.T) {
	t.Log("场景: 落盘格式比当前二进制新 -> 明确拒绝并说明原因")
	m, _ := newMgr(t, 3, 10)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := m.Save(samplePayload(base)); err != nil {
		t.Fatal(err)
	}
	files, _ := m.list()
	data, _ := os.ReadFile(files[0])
	// Rewrite the payload with a newer format version and re-seal the
	// checksum, so ONLY the version is "wrong".
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatal(err)
	}
	env.Payload = []byte(strings.Replace(string(env.Payload),
		`"format_version":1`, `"format_version":99`, 1))
	env.Checksum = crc32.ChecksumIEEE(env.Payload)
	sealed, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files[0], sealed, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = m.Load()
	if err == nil || !strings.Contains(err.Error(), "newer than supported") {
		t.Fatalf("want explicit newer-version refusal, got %v", err)
	}
	t.Logf("拒绝启动原因: %v", err)
}

func TestRetentionPrunesOldFiles(t *testing.T) {
	t.Log("场景: 留存份数有上限，超出后最旧的检查点被清理，空间不无限增长")
	m, dir := newMgr(t, 2, 10)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		if err := m.Save(samplePayload(base.Add(time.Duration(i) * time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "ckpt-") {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("retained %d files, want 2", count)
	}
	// Newest two survive: minutes 3 and 4.
	got, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !got.WrittenAt.Equal(base.Add(4 * time.Minute)) {
		t.Fatalf("newest = %v, want minute 4", got.WrittenAt)
	}
	size, err := m.DirSize()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("保留 %d 份，目录占用 %d 字节", count, size)
}

func TestEventHistoryBounded(t *testing.T) {
	t.Log("场景: 事件历史有保留上限，只保留最近的 N 条")
	m, _ := newMgr(t, 3, 3)
	p := samplePayload(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	p.Events = nil
	for i := 0; i < 10; i++ {
		p.Events = append(p.Events, model.AlertEvent{RuleID: "r", Value: float64(i)})
	}
	if err := m.Save(p); err != nil {
		t.Fatal(err)
	}
	got, err := m.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 3 || got.Events[0].Value != 7 {
		t.Fatalf("events = %+v, want newest 3 (values 7,8,9)", got.Events)
	}
}

func TestSaveDoesNotBlockOnConcurrentSave(t *testing.T) {
	t.Log("场景: 连续多次落盘互不干扰，文件始终完整可读")
	m, _ := newMgr(t, 3, 10)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	done := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func(i int) {
			done <- m.Save(samplePayload(base.Add(time.Duration(i) * time.Second)))
		}(i)
	}
	for i := 0; i < 8; i++ {
		if err := <-done; err != nil {
			t.Fatalf("concurrent Save: %v", err)
		}
	}
	if _, err := m.Load(); err != nil {
		t.Fatalf("Load after concurrent saves: %v", err)
	}
}
