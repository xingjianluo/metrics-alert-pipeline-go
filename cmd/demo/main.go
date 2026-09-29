// Command demo runs a local end-to-end demonstration of the metrics alert
// pipeline: concurrent writes, cardinality rejection, backpressure, rule
// hot-reload and an alert firing/recovering/resolving, followed by a graceful
// shutdown that drains every queued sample.
//
// Run with:  go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/aggregator"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/pipeline"
)

func main() {
	window := 40 * time.Millisecond

	cpuRule := model.Rule{
		ID: "cpu-high", Metric: "cpu",
		Agg: model.AggLast, Op: model.OpGT, Threshold: 80,
		Window: window, ForDuration: 2 * window, KeepDuration: 2 * window,
	}
	memRule := model.Rule{
		ID: "mem-high", Metric: "mem",
		MatchLabels: map[string]string{"kind": "ram"},
		Agg:         model.AggAvg, Op: model.OpGE, Threshold: 70,
		Window: window, ForDuration: 3 * window, KeepDuration: 2 * window,
	}

	p := pipeline.New(pipeline.Config{
		IngestQueueSize: 64,
		Backpressure:    pipeline.PolicyDropOldest,
		EvaluatorTick:   10 * time.Millisecond,
		LatencyBudget:   5 * time.Millisecond,
	}, aggregator.Config{
		Window:       window,
		MaxSeries:    6, // cpu a-d + mem + burst admitted; cpu host e rejected
		WatermarkLag: 20 * time.Millisecond,
	}, []model.Rule{cpuRule, memRule})

	// Observe outputs concurrently.
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for ev := range p.Events() {
			fmt.Printf("[event ] %-11s key=%s value=%.1f at=%s\n",
				ev.Type, ev.Key, ev.Value, ev.OccurredAt.Format("15:04:05.000"))
		}
	}()
	go func() {
		defer wg.Done()
		for d := range p.Drops() {
			fmt.Printf("[drop  ] reason=%-13s stage=%s metric=%s\n", d.Reason, d.Stage, d.Metric)
		}
	}()
	go func() {
		defer wg.Done()
		for r := range p.Rejects() {
			fmt.Printf("[reject] reason=%-28s %s\n", r.Reason, r.Detail)
		}
	}()

	p.Start(context.Background())

	stop := make(chan struct{})
	var producers sync.WaitGroup
	rng := rand.New(rand.NewSource(1))

	// 4 cpu producers: oscillate between hot and cool so the alert fires,
	// flaps and recovers. host e is a 6th series over the cardinality cap.
	for _, host := range []string{"a", "b", "c", "d", "e"} {
		producers.Add(1)
		go func(host string) {
			defer producers.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				// first ~300ms hot, then cool, then hot again (flap), then cool
				var v float64
				phase := i % 16
				switch {
				case phase < 6:
					v = 85 + rng.Float64()*10
				case phase < 10:
					v = 10 + rng.Float64()*5
				case phase < 13:
					v = 90 + rng.Float64()*5 // brief flap
				default:
					v = 5 + rng.Float64()*5
				}
				_ = p.Submit(model.Sample{
					Name:      "cpu",
					Labels:    map[string]string{"host": host},
					Value:     v,
					Timestamp: time.Now(),
				})
				i++
				time.Sleep(15 * time.Millisecond)
			}
		}(host)
	}

	// One mem producer that occasionally overloads to show backpressure drops.
	producers.Add(1)
	go func() {
		defer producers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = p.Submit(model.Sample{
				Name:      "mem",
				Labels:    map[string]string{"kind": "ram"},
				Value:     75,
				Timestamp: time.Now(),
			})
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// A short synchronous burst against the small queue demonstrates the
	// drop-oldest backpressure policy with an explicit queue_full reason.
	go func() {
		time.Sleep(150 * time.Millisecond)
		for i := 0; i < 500; i++ {
			_ = p.Submit(model.Sample{
				Name: "burst", Labels: map[string]string{"src": "burst"},
				Value: float64(i), Timestamp: time.Now(),
			})
		}
	}()

	// Hot-update the cpu threshold in the middle of a firing episode.
	time.Sleep(350 * time.Millisecond)
	cpuRule.Threshold = 60
	fmt.Println("[rules ] hot-reloading cpu-high threshold 80 -> 60 (state preserved)")
	p.UpdateRules([]model.Rule{cpuRule, memRule})

	time.Sleep(450 * time.Millisecond)
	close(stop)
	producers.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "shutdown error:", err)
		os.Exit(1)
	}
	wg.Wait()

	st := p.Stats()
	fmt.Println("[stats ] graceful shutdown drained all queued samples:")
	fmt.Printf("[stats ] submitted=%d aggregated=%d events=%d drops(full)=%d "+
		"drops(shutdown)=%d rejected=%d evalMax=%dµs budgetOverruns=%d\n",
		st.Submitted, st.Aggregated, st.EventsEmitted, st.DroppedFull,
		st.DroppedShutdown, st.Rejected, st.EvalMaxMicros, st.BudgetOverruns)
}
