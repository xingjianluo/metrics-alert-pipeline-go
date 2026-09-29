package pipeline

import (
	"testing"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/store"
)

func storeOpts() store.Options {
	return store.Options{WindowSize: time.Second, MaxSeries: 100, StaleWindows: 2}
}

// drainAt waits until cumulative processed (accepted+rejected) samples
// reach want, then runs a deterministic evaluation at synthetic now.
func (p *Pipeline) drainAt(t *testing.T, want int64, now time.Time) {
	t.Helper()
	waitFor(t, p, func() bool {
		s := p.Stats()
		return s.Accepted+s.Rejected == want
	})
	p.evaluateOnce(now)
}

func waitFor(t *testing.T, p *Pipeline, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met; stats=%+v", p.Stats())
}
