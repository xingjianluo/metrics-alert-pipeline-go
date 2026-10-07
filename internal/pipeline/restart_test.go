package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/checkpoint"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
)

// ckptConfig returns a deterministic config with persistence enabled.
func ckptConfig(t *testing.T, dir string, rs rule.Snapshot) Config {
	t.Helper()
	return detConfig(t, rs, func(c *Config) {
		c.CheckpointDir = dir
		c.CheckpointsKept = 5
	})
}

// feed submits one sample per second for secs and deterministically
// flushes each window. The drain target is derived from the pipeline's
// own counters, so it works across restarted pipeline instances.
func feed(t *testing.T, p *Pipeline, metric, host string, v float64, secs ...int64) {
	t.Helper()
	st := p.Stats()
	done := st.Accepted + st.Rejected
	for _, sec := range secs {
		if err := p.Submit(model.Sample{Name: metric,
			Labels: map[string]string{"host": host}, Value: v,
			Timestamp: time.Unix(sec, 0)}); err != nil {
			t.Fatal(err)
		}
		done++
		p.drainAt(t, done, time.Unix(sec+3, 0))
	}
}

func stopClean(t *testing.T, p *Pipeline) {
	t.Helper()
	ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
	defer c()
	if err := p.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRestartContinuesPendingTimer(t *testing.T) {
	t.Log("场景: 待触发计时跨重启延续, 重启不把 pending 计时清零")
	dir := t.TempDir()

	p1, err := New(ckptConfig(t, dir, baseRules()))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	// Two hot windows: pending since window-end 1, not yet firing.
	feed(t, p1, "cpu", "a", 9, 0, 1)
	if got := p1.Stats().FiringEmitted; got != 0 {
		t.Fatalf("p1 should not have fired yet, got %d", got)
	}
	stopClean(t, p1)

	p2, err := New(ckptConfig(t, dir, baseRules()))
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()
	// The restored pending run started at window-end 1: one more hot
	// window (end 3) completes ForDuration=2s. A reset timer would not
	// fire here.
	feed(t, p2, "cpu", "a", 9, 2)
	if got := p2.Stats().FiringEmitted; got != 1 {
		t.Fatalf("pending timer must survive restart: want 1 firing, got %d", got)
	}
	stopClean(t, p2)
}

func TestRestartDoesNotRefireOrLoseResolve(t *testing.T) {
	t.Log("场景: 已触发的告警重启后不重复发 Firing, 恢复计时延续且 Resolved 恰好一次")
	dir := t.TempDir()

	p1, err := New(ckptConfig(t, dir, baseRules()))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	feed(t, p1, "cpu", "a", 9, 0, 1, 2) // fires at window-end 3
	if got := p1.Stats().FiringEmitted; got != 1 {
		t.Fatalf("p1 want 1 firing, got %d", got)
	}
	stopClean(t, p1)

	p2, err := New(ckptConfig(t, dir, baseRules()))
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()
	// Flap inside the same episode: hot (still firing), cold
	// (resolving), hot (flap back, no event), then sustained cold.
	feed(t, p2, "cpu", "a", 9, 3)
	feed(t, p2, "cpu", "a", 1, 4)
	feed(t, p2, "cpu", "a", 9, 5)
	feed(t, p2, "cpu", "a", 1, 6, 7, 8)
	st := p2.Stats()
	if st.FiringEmitted != 0 {
		t.Fatalf("episode must continue across restart: want 0 new firings, got %d", st.FiringEmitted)
	}
	if st.ResolvedEmitted != 1 {
		t.Fatalf("resolve must not be lost across restart: want 1 resolved, got %d", st.ResolvedEmitted)
	}
	stopClean(t, p2)
}

func TestRestartWithChangedRuleset(t *testing.T) {
	t.Log("场景: 重启+规则热更新组合: 同 ID 规则(改了阈值)状态延续, 删除的规则不再参与, 新增规则从干净状态起步")
	dir := t.TempDir()

	v1 := rule.Snapshot{Version: 1, Rules: []rule.Rule{
		{ID: "rule_a", Metric: "cpu", Field: rule.FieldAvg, Op: rule.OpGT,
			Threshold: 5, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second},
		{ID: "rule_b", Metric: "mem", Field: rule.FieldAvg, Op: rule.OpGT,
			Threshold: 5, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second},
	}}
	p1, err := New(ckptConfig(t, dir, v1))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	feed(t, p1, "cpu", "a", 9, 0, 1, 2) // rule_a fires
	feed(t, p1, "mem", "a", 9, 0, 1, 2) // rule_b fires
	if got := p1.Stats().FiringEmitted; got != 2 {
		t.Fatalf("p1 want 2 firings, got %d", got)
	}
	stopClean(t, p1)

	// v2: rule_a threshold changed, rule_b deleted, rule_c added.
	v2 := rule.Snapshot{Version: 2, Rules: []rule.Rule{
		{ID: "rule_a", Metric: "cpu", Field: rule.FieldAvg, Op: rule.OpGT,
			Threshold: 1, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second},
		{ID: "rule_c", Metric: "disk", Field: rule.FieldAvg, Op: rule.OpGT,
			Threshold: 5, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second},
	}}
	var mu sync.Mutex
	var sink []model.AlertEvent
	cfg := ckptConfig(t, dir, v2)
	cfg.EventSink = func(ev model.AlertEvent) {
		mu.Lock()
		sink = append(sink, ev)
		mu.Unlock()
	}
	p2, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()
	feed(t, p2, "cpu", "a", 0, 3, 4, 5) // rule_a resolves (episode continued)
	feed(t, p2, "mem", "a", 9, 3, 4, 5) // rule_b gone: no events
	feed(t, p2, "disk", "a", 9, 3, 4, 5)
	// rule_c starts clean: needs the full ForDuration, fires at end 6.
	feed(t, p2, "disk", "a", 9, 6)

	mu.Lock()
	defer mu.Unlock()
	var aResolved, cFiring, bEvents int
	for _, ev := range sink {
		switch {
		case ev.RuleID == "rule_a" && ev.State == model.StateInactive:
			aResolved++
		case ev.RuleID == "rule_c" && ev.State == model.StateFiring:
			cFiring++
		case ev.RuleID == "rule_b":
			bEvents++
		}
	}
	if aResolved != 1 {
		t.Fatalf("rule_a episode must continue across restart+update: want 1 resolved, got %d", aResolved)
	}
	if cFiring != 1 {
		t.Fatalf("rule_c must start clean and fire once: got %d", cFiring)
	}
	if bEvents != 0 {
		t.Fatalf("deleted rule_b must not participate: got %d events", bEvents)
	}
	st := p2.Stats()
	if st.FiringEmitted != 1 {
		t.Fatalf("rule_a must not re-fire after restart: total firings want 1 (rule_c), got %d", st.FiringEmitted)
	}
	stopClean(t, p2)
}

func TestCorruptOnlyCheckpointRefusesStartup(t *testing.T) {
	t.Log("场景: 唯一检查点损坏, 启动明确失败并说清原因, 不静默按空状态跑")
	dir := t.TempDir()
	p1, err := New(ckptConfig(t, dir, baseRules()))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	feed(t, p1, "cpu", "a", 9, 0, 1, 2)
	stopClean(t, p1)

	files, _ := filepath.Glob(filepath.Join(dir, "checkpoint-*.json"))
	if len(files) == 0 {
		t.Fatal("expected a checkpoint file after stop")
	}
	for _, f := range files {
		if err := os.WriteFile(f, []byte("{corrupted"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	_, err = New(ckptConfig(t, dir, baseRules()))
	if err == nil {
		t.Fatal("startup must fail when every checkpoint is corrupt")
	}
	if !strings.Contains(err.Error(), "no usable checkpoint") {
		t.Fatalf("error must explain the reason, got: %v", err)
	}
	t.Logf("启动拒绝原因: %v", err)
}

func TestNewestCheckpointCorruptFallsBackAtStartup(t *testing.T) {
	t.Log("场景: 最新检查点损坏时启动回退到上一份可用检查点, 状态停留在该时点")
	dir := t.TempDir()
	cfg := ckptConfig(t, dir, baseRules())
	p1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	feed(t, p1, "cpu", "a", 9, 0, 1) // pending since window-end 1
	p1.saveCheckpoint()              // checkpoint 1: pending
	feed(t, p1, "cpu", "a", 9, 2)    // fires
	stopClean(t, p1)                 // checkpoint 2: firing

	// Corrupt the newest checkpoint only.
	files, _ := filepath.Glob(filepath.Join(dir, "checkpoint-*.json"))
	if len(files) != 2 {
		t.Fatalf("want 2 checkpoints, got %v", files)
	}
	newest := files[len(files)-1]
	fi, _ := os.Stat(newest)
	if err := os.Truncate(newest, fi.Size()/2); err != nil {
		t.Fatal(err)
	}

	p2, err := New(ckptConfig(t, dir, baseRules()))
	if err != nil {
		t.Fatalf("fallback must allow startup, got %v", err)
	}
	p2.Start()
	// Restored from checkpoint 1: pending, not firing. One more hot
	// window completes the duration and fires exactly once.
	feed(t, p2, "cpu", "a", 9, 2)
	if got := p2.Stats().FiringEmitted; got != 1 {
		t.Fatalf("after fallback want 1 firing, got %d", got)
	}
	stopClean(t, p2)
}

func TestConcurrentWritesDuringCheckpointing(t *testing.T) {
	t.Log("场景: 恢复后并发写入与周期落盘同时进行, -race 下稳定且计数精确")
	dir := t.TempDir()
	p1, err := New(ckptConfig(t, dir, baseRules()))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	feed(t, p1, "cpu", "seed", 9, 0, 1, 2)
	stopClean(t, p1)

	p2, err := New(ckptConfig(t, dir, baseRules()))
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()

	const writers = 8
	const perWriter = 50
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				// All samples stay inside the staleness bound (two
				// windows); distinct nanoseconds keep timestamps
				// unique inside each series.
				ts := time.Unix(1000+int64(i%2), int64((i*writers+w)*1000))
				_ = p2.Submit(model.Sample{Name: "cpu",
					Labels: map[string]string{"host": string(rune('a' + w))},
					Value:  9, Timestamp: ts})
			}
		}(w)
	}
	// Concurrent checkpointing and evaluation while writes land.
	for i := 0; i < 20; i++ {
		p2.saveCheckpoint()
		p2.FlushNow(time.Unix(1003+int64(i*3), 0))
	}
	wg.Wait()
	waitFor(t, p2, func() bool {
		s := p2.Stats()
		return s.Accepted+s.Rejected == writers*perWriter
	})
	stopClean(t, p2)
	st := p2.Stats()
	if st.Accepted != writers*perWriter {
		t.Fatalf("want %d accepted samples, got %d (rejected %d)",
			writers*perWriter, st.Accepted, st.Rejected)
	}

	// The final checkpoint written after concurrent saves must load.
	mgr, err := checkpoint.NewManager(dir, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Load(); err != nil {
		t.Fatalf("checkpoint written under concurrency must be loadable: %v", err)
	}
}

func TestEventHistoryRetentionAcrossRestart(t *testing.T) {
	t.Log("场景: 事件历史有保留上限, 超限丢弃最旧, 重启后保留的历史不膨胀")
	dir := t.TempDir()
	cfg := ckptConfig(t, dir, baseRules())
	cfg.MaxEventsKept = 2
	p1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	// Two full fire->resolve cycles = 4 events, cap keeps the last 2.
	feed(t, p1, "cpu", "a", 9, 0, 1, 2)
	feed(t, p1, "cpu", "a", 1, 3, 4, 5)
	feed(t, p1, "cpu", "a", 9, 6, 7, 8)
	feed(t, p1, "cpu", "a", 1, 9, 10, 11)
	if got := len(p1.RecentEvents()); got != 2 {
		t.Fatalf("event history must be capped at 2, got %d", got)
	}
	stopClean(t, p1)

	p2, err := New(ckptConfig(t, dir, baseRules()))
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()
	evs := p2.RecentEvents()
	if len(evs) != 2 {
		t.Fatalf("restored event history must stay capped at 2, got %d", len(evs))
	}
	// The retained events are the most recent fire + resolve.
	if evs[0].State != model.StateFiring || evs[1].State != model.StateInactive {
		t.Fatalf("retained events should be latest fire+resolve, got %v %v",
			evs[0].State, evs[1].State)
	}
	stopClean(t, p2)
}
