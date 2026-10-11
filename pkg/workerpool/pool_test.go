package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool_Execution(t *testing.T) {
	pool := New(Config{
		Workers:   4,
		QueueSize: 20,
	})

	const taskCount = 20
	var sum atomic.Int64

	for i := 1; i <= taskCount; i++ {
		val := int64(i)
		err := pool.Submit(NewTask(fmt.Sprintf("task-%d", i), func(ctx context.Context) (any, error) {
			sum.Add(val)
			return val * 2, nil
		}))
		if err != nil {
			t.Fatalf("failed to submit task: %v", err)
		}
	}

	go pool.Stop()

	var receivedCount int
	for res := range pool.Results() {
		if res.Err != nil {
			t.Errorf("unexpected error in task %s: %v", res.TaskID, res.Err)
		}
		receivedCount++
	}

	if receivedCount != taskCount {
		t.Errorf("expected %d results, got %d", taskCount, receivedCount)
	}

	expectedSum := int64(taskCount * (taskCount + 1) / 2)
	if sum.Load() != expectedSum {
		t.Errorf("expected sum %d, got %d", expectedSum, sum.Load())
	}

	metrics := pool.Metrics()
	if metrics.Completed != int64(taskCount) {
		t.Errorf("expected %d completed metrics, got %d", taskCount, metrics.Completed)
	}
}

func TestWorkerPool_PanicRecovery(t *testing.T) {
	pool := New(Config{
		Workers:   2,
		QueueSize: 5,
	})

	err := pool.Submit(NewTask("panic-task", func(ctx context.Context) (any, error) {
		panic("fatal calculation error inside task")
	}))
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// Normal task right after to ensure pool workers didn't die
	err = pool.Submit(NewTask("normal-task", func(ctx context.Context) (any, error) {
		return "ok", nil
	}))
	if err != nil {
		t.Fatalf("submit normal failed: %v", err)
	}

	go pool.Stop()

	var panicsFound int
	for res := range pool.Results() {
		if res.TaskID == "panic-task" {
			if res.Err == nil {
				t.Errorf("expected error from panic task, got nil")
			}
			panicsFound++
		}
	}

	if panicsFound != 1 {
		t.Errorf("expected 1 panic result, got %d", panicsFound)
	}

	metrics := pool.Metrics()
	if metrics.Panics != 1 {
		t.Errorf("expected 1 panic recorded, got %d", metrics.Panics)
	}
	if metrics.Completed != 1 {
		t.Errorf("expected 1 completed normal task, got %d", metrics.Completed)
	}
}

func TestWorkerPool_GracefulShutdown(t *testing.T) {
	pool := New(Config{
		Workers:   2,
		QueueSize: 10,
	})

	for i := 0; i < 5; i++ {
		id := i
		_ = pool.Submit(NewTask(fmt.Sprintf("t-%d", id), func(ctx context.Context) (any, error) {
			time.Sleep(20 * time.Millisecond)
			return id, nil
		}))
	}

	// Immediate stop - should process all 5 enqueued tasks
	pool.Stop()

	// New submission after Stop should return ErrPoolClosed
	err := pool.Submit(NewTask("rejected", func(ctx context.Context) (any, error) {
		return nil, nil
	}))
	if !errors.Is(err, ErrPoolClosed) {
		t.Errorf("expected ErrPoolClosed, got %v", err)
	}

	var count int
	for range pool.Results() {
		count++
	}

	if count != 5 {
		t.Errorf("expected 5 drained tasks, got %d", count)
	}
}

func BenchmarkWorkerPool(b *testing.B) {
	pool := New(Config{
		Workers:   8,
		QueueSize: 1000,
	})

	go func() {
		for range pool.Results() {
			// drain
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pool.Submit(NewTask("bench", func(ctx context.Context) (any, error) {
			return 42, nil
		}))
	}
	pool.Stop()
}
