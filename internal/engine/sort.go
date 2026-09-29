package engine

import (
	"sort"

	"github.com/xingjianluo/metrics-alert-pipeline-go/internal/model"
)

type evalItem struct {
	agg model.WindowAggregate
	hit bool
}

func sortItems(items []evalItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].agg.Series != items[j].agg.Series {
			return items[i].agg.Series < items[j].agg.Series
		}
		return items[i].agg.WindowStart.Before(items[j].agg.WindowStart)
	})
}

func valueForLog(agg model.WindowAggregate, f string) float64 {
	switch f {
	case "sum":
		return agg.Sum
	case "min":
		return agg.Min
	case "max":
		return agg.Max
	case "latest":
		return agg.Latest
	default:
		if agg.Count == 0 {
			return 0
		}
		return agg.Sum / float64(agg.Count)
	}
}
