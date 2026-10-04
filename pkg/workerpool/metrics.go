package workerpool

import (
	"sync/atomic"
)

// Metrics maintains thread-safe execution counters.
type Metrics struct {
	submitted atomic.Int64
	completed atomic.Int64
	failed    atomic.Int64
	panics    atomic.Int64
}

// Snapshot provides a point-in-time copy of pool statistics.
type Snapshot struct {
	Submitted int64 `json:"submitted"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
	Panics    int64 `json:"panics"`
}

func (m *Metrics) incSubmitted() {
	m.submitted.Add(1)
}

func (m *Metrics) incCompleted() {
	m.completed.Add(1)
}

func (m *Metrics) incFailed() {
	m.failed.Add(1)
}

func (m *Metrics) incPanics() {
	m.panics.Add(1)
}

// Snapshot returns the current metric values.
func (m *Metrics) Snapshot() Snapshot {
	return Snapshot{
		Submitted: m.submitted.Load(),
		Completed: m.completed.Load(),
		Failed:    m.failed.Load(),
		Panics:    m.panics.Load(),
	}
}
