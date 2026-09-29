package model

import "sort"

// MakeSeriesKey renders the canonical key of a metric name and label set.
// Label keys are sorted so map iteration order never affects identity.
func MakeSeriesKey(name string, labels map[string]string) SeriesKey {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	b := make([]byte, 0, len(name)+len(keys)*16)
	b = append(b, name...)
	b = append(b, '|')
	for i, k := range keys {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, labels[k]...)
	}
	return SeriesKey(b)
}
