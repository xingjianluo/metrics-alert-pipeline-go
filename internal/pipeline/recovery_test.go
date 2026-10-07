package pipeline

import (
	"context"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/rule"
)

// ckptConfig builds a deterministic config with persistence enabled.
func ckptConfig(t *testing.T, dir string, rs rule.Snapshot, mutate func(*Config)) Config {
	t.Helper()
	return detConfig(t, rs, func(c *Config) {
		c.CheckpointDir = dir
		c.CheckpointInterval = time.Hour // driven manually via SaveCheckpointNow
		if mutate != nil {
			mutate(c)
		}
	})
}

func stopNow(t *testing.T, p *Pipeline) {
	t.Helper()
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// submit one hot/cold cpu sample for host "a" at second sec and evaluate
// deterministically.
func stepCPU(t *testing.T, p *Pipeline, processed *int64, sec int64, v float64) {
	t.Helper()
	if err := p.Submit(model.Sample{Name: "cpu",
		Labels: map[string]string{"host": "a"}, Value: v,
		Timestamp: time.Unix(sec, 0)}); err != nil {
		t.Fatal(err)
	}
	*processed++
	p.drainAt(t, *processed, time.Unix(sec+3, 0))
}

func TestRestartContinuesPendingAndFiresOnce(t *testing.T) {
	t.Log("场景: 重启不清零待触发计时 —— 重启前 2 个命中窗口，重启后第 3 个命中窗口即触发，且全程只触发一次")
	dir := t.TempDir()

	// Phase 1: two hot windows -> pending (run = 1s of the 2s ForDuration).
	p1, err := New(ckptConfig(t, dir, baseRules(), nil))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	var n1 int64
	stepCPU(t, p1, &n1, 0, 9)
	stepCPU(t, p1, &n1, 1, 9)
	if got := p1.Stats().FiringEmitted; got != 0 {
		t.Fatalf("fired too early: %d", got)
	}
	p1.SaveCheckpointNow()
	stopNow(t, p1)

	// Phase 2: restart with the same ruleset; the third hot window must
	// fire immediately (pending timer survived), exactly once.
	p2, err := New(ckptConfig(t, dir, baseRules(), nil))
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()
	var n2 int64
	stepCPU(t, p2, &n2, 2, 9)
	waitFor(t, p2, func() bool { return p2.Stats().FiringEmitted == 1 })
	if got := p1.Stats().FiringEmitted + p2.Stats().FiringEmitted; got != 1 {
		t.Fatalf("total firing events across restart = %d, want 1", got)
	}

	// Recovery continues across the restart boundary too: cold windows
	// resolve exactly once, nothing is lost.
	for sec := int64(3); sec <= 5; sec++ {
		stepCPU(t, p2, &n2, sec, 1)
	}
	waitFor(t, p2, func() bool { return p2.Stats().ResolvedEmitted == 1 })
	stopNow(t, p2)
}

func TestRestartNoDuplicateFiring(t *testing.T) {
	t.Log("场景: 已触发未恢复的告警重启后不重发 Firing；恢复计时延续，Resolved 不丢")
	dir := t.TempDir()

	// Phase 1: fire the alert, then checkpoint while the episode is open.
	p1, err := New(ckptConfig(t, dir, baseRules(), nil))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	var n1 int64
	for sec := int64(0); sec <= 2; sec++ {
		stepCPU(t, p1, &n1, sec, 9)
	}
	waitFor(t, p1, func() bool { return p1.Stats().FiringEmitted == 1 })
	p1.SaveCheckpointNow()
	stopNow(t, p1)

	// Phase 2: the condition is still true but the episode was already
	// announced -> no second Firing. Then cold windows resolve once.
	p2, err := New(ckptConfig(t, dir, baseRules(), nil))
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()
	var n2 int64
	stepCPU(t, p2, &n2, 3, 9) // still hot: flap-free continuation
	if got := p2.Stats().FiringEmitted; got != 0 {
		t.Fatalf("duplicate Firing after restart: %d", got)
	}
	for sec := int64(4); sec <= 6; sec++ {
		stepCPU(t, p2, &n2, sec, 1)
	}
	waitFor(t, p2, func() bool { return p2.Stats().ResolvedEmitted == 1 })
	if got := p2.Stats().FiringEmitted; got != 0 {
		t.Fatalf("late duplicate Firing: %d", got)
	}

	// The event history survived the restart (dedup basis + audit).
	hist := p2.EventHistory()
	if len(hist) == 0 || hist[0].State != model.StateFiring {
		t.Fatalf("event history lost across restart: %+v", hist)
	}
	t.Logf("重启后事件历史保留 %d 条，首条: %s", len(hist), hist[0].State)
	stopNow(t, p2)
}

func TestRestartUnflushedWindowsSurvive(t *testing.T) {
	t.Log("场景: 未判定完的窗口数据随检查点恢复 —— 重启前 1 个命中窗口未 flush，重启后接着算")
	dir := t.TempDir()

	// p1 uses a synthetic clock pinned at t=1s so the graceful-shutdown
	// final flush cannot deliver window 0 (readyAt = 3s): the window is
	// still unflushed when the checkpoint is written.
	p1, err := New(ckptConfig(t, dir, baseRules(), func(c *Config) {
		c.Clock = func() time.Time { return time.Unix(1, 0) }
	}))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	if err := p1.Submit(model.Sample{Name: "cpu",
		Labels: map[string]string{"host": "a"}, Value: 9,
		Timestamp: time.Unix(0, 0)}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, p1, func() bool { return p1.Stats().Accepted == 1 })
	p1.SaveCheckpointNow()
	stopNow(t, p1) // final eval at t=1s flushes nothing

	// p2: windows 1 and 2 arrive hot. If window 0 had been lost, the
	// pending run would start at window 1 and NOT fire at window 2.
	p2, err := New(ckptConfig(t, dir, baseRules(), nil))
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()
	var n2 int64
	stepCPU(t, p2, &n2, 1, 9)
	stepCPU(t, p2, &n2, 2, 9)
	waitFor(t, p2, func() bool { return p2.Stats().FiringEmitted == 1 })
	t.Log("未 flush 的窗口 0 在重启后参与判定，第 3 个命中窗口按时触发")
	stopNow(t, p2)
}

func TestRestartWithChangedRuleset(t *testing.T) {
	t.Log("场景: 检查点 + 规则变更 —— 同 ID 规则(改了阈值)延续状态，被删规则退役，新增规则从干净状态起步")
	dir := t.TempDir()

	rulesV1 := rule.Snapshot{Version: 1, Rules: []rule.Rule{
		{ID: "hot_cpu", Metric: "cpu", Field: rule.FieldAvg, Op: rule.OpGT,
			Threshold: 5, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second},
		{ID: "hot_mem", Metric: "mem", Field: rule.FieldAvg, Op: rule.OpGT,
			Threshold: 5, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second},
	}}
	p1, err := New(ckptConfig(t, dir, rulesV1, nil))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	var n1 int64
	// cpu: 2 hot windows -> pending. mem: 3 hot windows -> firing.
	stepCPU(t, p1, &n1, 0, 9)
	stepCPU(t, p1, &n1, 1, 9)
	for sec := int64(0); sec <= 2; sec++ {
		if err := p1.Submit(model.Sample{Name: "mem",
			Labels: map[string]string{"host": "a"}, Value: 9,
			Timestamp: time.Unix(sec, 0)}); err != nil {
			t.Fatal(err)
		}
		n1++
		p1.drainAt(t, n1, time.Unix(sec+3, 0))
	}
	waitFor(t, p1, func() bool { return p1.Stats().FiringEmitted == 1 }) // mem fired
	p1.SaveCheckpointNow()
	stopNow(t, p1)

	// v2: hot_cpu kept with a HIGHER threshold (9 no longer matches),
	// hot_mem deleted, hot_disk added.
	rulesV2 := rule.Snapshot{Version: 2, Rules: []rule.Rule{
		{ID: "hot_cpu", Metric: "cpu", Field: rule.FieldAvg, Op: rule.OpGT,
			Threshold: 50, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second},
		{ID: "hot_disk", Metric: "disk", Field: rule.FieldAvg, Op: rule.OpGT,
			Threshold: 5, ForDuration: 2 * time.Second, KeepResolvedFor: 2 * time.Second},
	}}
	p2, err := New(ckptConfig(t, dir, rulesV2, nil))
	if err != nil {
		t.Fatal(err)
	}
	p2.Start()

	// Deleted rule's state was retired: engine only tracks v2 rule IDs.
	for _, v := range p2.Engine().Snapshot() {
		if v.RuleID == "hot_mem" {
			t.Fatalf("state of deleted rule survived: %+v", v)
		}
	}

	var n2 int64
	// Same-ID rule with changed threshold: state is preserved, the new
	// predicate applies. cpu=9 no longer matches threshold 50 -> the
	// pending run is discarded (explainable: condition now false), and
	// crucially no spurious Firing is emitted from the restored state.
	stepCPU(t, p2, &n2, 2, 9)
	if got := p2.Stats().FiringEmitted; got != 0 {
		t.Fatalf("spurious firing from restored pending state: %d", got)
	}
	// New rule starts clean: one hot disk window must NOT fire.
	if err := p2.Submit(model.Sample{Name: "disk",
		Labels: map[string]string{"host": "a"}, Value: 9,
		Timestamp: time.Unix(2, 0)}); err != nil {
		t.Fatal(err)
	}
	n2++
	p2.drainAt(t, n2, time.Unix(5, 0))
	if got := p2.Stats().FiringEmitted; got != 0 {
		t.Fatalf("new rule fired without full ForDuration: %d", got)
	}
	// ... and fires only after its own full 2s run.
	for sec := int64(3); sec <= 4; sec++ {
		if err := p2.Submit(model.Sample{Name: "disk",
			Labels: map[string]string{"host": "a"}, Value: 9,
			Timestamp: time.Unix(sec, 0)}); err != nil {
			t.Fatal(err)
		}
		n2++
		p2.drainAt(t, n2, time.Unix(sec+3, 0))
	}
	waitFor(t, p2, func() bool { return p2.Stats().FiringEmitted == 1 })
	t.Log("同 ID 规则状态延续、删除规则退役、新增规则干净起步均符合预期")
	stopNow(t, p2)
}

func TestCorruptCheckpointStartupBehavior(t *testing.T) {
	t.Log("场景: 启动时检查点损坏 —— 有备份则回退续算，全部损坏则拒绝启动并说明原因")
	dir := t.TempDir()

	// Produce three checkpoints: A = clean (before any sample),
	// B = pending after 2 hot windows, C = shutdown save (same as B).
	p1, err := New(ckptConfig(t, dir, baseRules(), nil))
	if err != nil {
		t.Fatal(err)
	}
	p1.Start()
	p1.SaveCheckpointNow() // A: clean
	time.Sleep(2 * time.Millisecond)
	var n1 int64
	stepCPU(t, p1, &n1, 0, 9)
	stepCPU(t, p1, &n1, 1, 9)
	p1.SaveCheckpointNow() // B: pending, run = 2 windows
	stopNow(t, p1)         // C: shutdown save

	// Corrupt the TWO newest checkpoints (B and C) -> restart must fall
	// back to A (clean). The fallback point is observable: from A the
	// alert needs a full 3-hot-window run, from B it would fire on the
	// very first hot window after restart.
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "ckpt-") {
			names = append(names, e.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	if len(names) < 3 {
		t.Fatalf("expected >= 3 checkpoints (2 manual + 1 shutdown), got %v", names)
	}
	for _, n := range names[:2] {
		if err := os.WriteFile(dir+"/"+n, []byte("{corrupt"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	p2, err := New(ckptConfig(t, dir, baseRules(), nil))
	if err != nil {
		t.Fatalf("should fall back to previous checkpoint, got: %v", err)
	}
	p2.Start()
	var n2 int64
	stepCPU(t, p2, &n2, 2, 9)
	stepCPU(t, p2, &n2, 3, 9)
	if got := p2.Stats().FiringEmitted; got != 0 {
		t.Fatalf("fired too early: fallback to clean checkpoint requires a full run, got %d", got)
	}
	stepCPU(t, p2, &n2, 4, 9)
	waitFor(t, p2, func() bool { return p2.Stats().FiringEmitted == 1 })
	t.Log("最新两份检查点损坏，回退到干净检查点重新累计，触发点符合回退点的状态")
	stopNow(t, p2)

	// Corrupt EVERYTHING -> New must refuse with an explicit reason.
	ents, _ = os.ReadDir(dir)
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "ckpt-") {
			if err := os.WriteFile(dir+"/"+e.Name(), []byte("{corrupt"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	_, err = New(ckptConfig(t, dir, baseRules(), nil))
	if err == nil || !strings.Contains(err.Error(), "refusing to start empty") {
		t.Fatalf("want explicit startup refusal, got %v", err)
	}
	t.Logf("全部损坏时拒绝启动: %v", err)
}

func TestCheckpointWithConcurrentWrites(t *testing.T) {
	t.Log("场景: 周期落盘与并发写入/判定同时进行 —— -race 下稳定，检查点始终可读")
	dir := t.TempDir()
	p, err := New(ckptConfig(t, dir, baseRules(), func(c *Config) {
		c.CheckpointInterval = 5 * time.Millisecond // real ticker
		c.IngestWorker = 4
	}))
	if err != nil {
		t.Fatal(err)
	}
	p.Start()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = p.Submit(model.Sample{Name: "cpu",
					Labels:    map[string]string{"host": "a"},
					Value:     float64(g + i%10),
					Timestamp: time.Now()})
			}
		}(g)
	}
	time.Sleep(120 * time.Millisecond) // several checkpoint ticks
	close(stop)
	wg.Wait()
	stopNow(t, p) // final checkpoint on shutdown

	// The directory holds a bounded number of loadable checkpoints.
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
	if count == 0 || count > 3 {
		t.Fatalf("retained checkpoints = %d, want 1..3", count)
	}
	// A fresh pipeline on the same dir starts cleanly from the last
	// checkpoint while writes were in flight.
	p2, err := New(ckptConfig(t, dir, baseRules(), nil))
	if err != nil {
		t.Fatalf("restart after concurrent checkpointing: %v", err)
	}
	stopNow(t, p2)
	t.Logf("并发落盘期间保留 %d 份检查点，重启加载正常", count)
}
