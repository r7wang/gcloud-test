package timer

import (
	"log"
	"time"
)

// Metrics provides utilities to track performance metrics by metric name.
type Metrics struct {
	durByName map[string][]int64
}

// NewMetrics returns a new Metrics instance.
func NewMetrics() *Metrics {
	return &Metrics{
		durByName: make(map[string][]int64),
	}
}

// Track keeps track of time taken to run an operation function.
func (m *Metrics) Track(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

// Summarize aggregates the metric results into a human-readable string.
func (m *Metrics) Summarize() string {
	return ""
}
