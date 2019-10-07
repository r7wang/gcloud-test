package timer

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/montanaflynn/stats"
)

// Metrics provides utilities to track performance metrics by metric name.
type Metrics struct {
	durationsByName map[string][]int64
}

// NewMetrics returns a new Metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{
		durationsByName: make(map[string][]int64),
	}
}

// Track keeps track of time taken to run an operation function.
func (m *Metrics) Track(start time.Time, name string) {
	elapsed := time.Since(start)
	if _, ok := m.durationsByName[name]; !ok {
		m.durationsByName[name] = []int64{}
	}
	m.durationsByName[name] = append(m.durationsByName[name], elapsed.Nanoseconds())
	log.Printf("(%d) %s took %s", len(m.durationsByName[name]), name, elapsed)
}

// Summarize aggregates the metric results into a human-readable string.
func (m *Metrics) Summarize() (string, error) {
	const minSamples = 100
	// Databases may require some number of samples to become "hot" and be able to handle requests
	// with consistent performance. Any metric without enough samples is omitted.
	const ignoredSamples = 10
	const nanosInMillis float64 = 1000000

	summaries := []string{}
	for name, durationsForName := range m.durationsByName {
		numSamples := len(durationsForName)
		if numSamples < minSamples {
			continue
		}
		raw := stats.LoadRawData(durationsForName[ignoredSamples:])
		mean, err := stats.Mean(raw)
		if err != nil {
			return "", err
		}
		median, err := stats.Median(raw)
		if err != nil {
			return "", err
		}
		pct75, err := stats.Percentile(raw, 75)
		if err != nil {
			return "", err
		}
		pct99, err := stats.Percentile(raw, 99)
		if err != nil {
			return "", err
		}
		summary := fmt.Sprintf("%s: samples=%d, mean=%.2f, median=%.2f, pct75=%.2f, pct99=%.2f",
			name,
			len(raw),
			mean/nanosInMillis,
			median/nanosInMillis,
			pct75/nanosInMillis,
			pct99/nanosInMillis)
		summaries = append(summaries, summary)
	}
	return strings.Join(summaries, "\n"), nil
}
