package workerpool

import (
	"context"
	"time"
)

// TaskFunc defines the execution signature for a job.
type TaskFunc func(ctx context.Context) (any, error)

// Task represents an executable unit of work.
type Task struct {
	ID        string
	Execute   TaskFunc
	CreatedAt time.Time
}

// Result holds the outcome of a Task execution.
type Result struct {
	TaskID    string
	Output    any
	Err       error
	Duration  time.Duration
	Completed time.Time
}

// NewTask creates a new Task with the current timestamp.
func NewTask(id string, fn TaskFunc) Task {
	return Task{
		ID:        id,
		Execute:   fn,
		CreatedAt: time.Now(),
	}
}
