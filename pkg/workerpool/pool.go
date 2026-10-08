package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrPoolClosed is returned when attempting to submit tasks to a closed pool.
	ErrPoolClosed = errors.New("workerpool: pool is closed")
	// ErrQueueFull is returned when non-blocking submission fails due to capacity.
	ErrQueueFull = errors.New("workerpool: task queue is full")
)

// Pool coordinates a fixed number of worker goroutines consuming tasks from a channel.
type Pool struct {
	workersCount int
	taskQueue    chan Task
	resultsQueue chan Result

	rpsLimit int
	limiter  *time.Ticker

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
	isClosed  atomic.Bool

	metrics Metrics
}

// Config specifies pool initialization parameters.
type Config struct {
	Workers   int // Number of worker goroutines
	QueueSize int // Capacity of the task queue buffer
	RPS       int // Optional rate limit (tasks started per second; 0 = unlimited)
}

// New creates and starts a new WorkerPool based on config.
func New(cfg Config) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.QueueSize < 0 {
		cfg.QueueSize = 0
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &Pool{
		workersCount: cfg.Workers,
		taskQueue:    make(chan Task, cfg.QueueSize),
		resultsQueue: make(chan Result, cfg.QueueSize),
		rpsLimit:     cfg.RPS,
		ctx:          ctx,
		cancel:       cancel,
	}

	if cfg.RPS > 0 {
		interval := time.Second / time.Duration(cfg.RPS)
		if interval <= 0 {
			interval = time.Millisecond
		}
		p.limiter = time.NewTicker(interval)
	}

	p.start()
	return p
}

func (p *Pool) start() {
	for i := 0; i < p.workersCount; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return

		case task, ok := <-p.taskQueue:
			if !ok {
				return
			}

			// Apply rate limiting if configured
			if p.limiter != nil {
				select {
				case <-p.ctx.Done():
					return
				case <-p.limiter.C:
				}
			}

			p.executeTask(task)
		}
	}
}

func (p *Pool) executeTask(task Task) {
	start := time.Now()
	var (
		output any
		err    error
	)

	// Safe execution with panic recovery
	func() {
		defer func() {
			if r := recover(); r != nil {
				p.metrics.incPanics()
				p.metrics.incFailed()
				err = fmt.Errorf("task panicked: %v", r)
			}
		}()

		output, err = task.Execute(p.ctx)
	}()

	duration := time.Since(start)

	if err != nil {
		p.metrics.incFailed()
	} else {
		p.metrics.incCompleted()
	}

	res := Result{
		TaskID:    task.ID,
		Output:    output,
		Err:       err,
		Duration:  duration,
		Completed: time.Now(),
	}

	select {
	case p.resultsQueue <- res:
	case <-p.ctx.Done():
	}
}

// Submit enqueues a task for processing. Blocks if the queue is full.
func (p *Pool) Submit(task Task) error {
	if p.isClosed.Load() {
		return ErrPoolClosed
	}

	select {
	case <-p.ctx.Done():
		return ErrPoolClosed
	case p.taskQueue <- task:
		p.metrics.incSubmitted()
		return nil
	}
}

// TrySubmit attempts to submit a task without blocking.
func (p *Pool) TrySubmit(task Task) error {
	if p.isClosed.Load() {
		return ErrPoolClosed
	}

	select {
	case <-p.ctx.Done():
		return ErrPoolClosed
	case p.taskQueue <- task:
		p.metrics.incSubmitted()
		return nil
	default:
		return ErrQueueFull
	}
}

// Results exposes the read-only channel where task outcomes are delivered.
func (p *Pool) Results() <-chan Result {
	return p.resultsQueue
}

// Metrics returns runtime telemetry for the pool.
func (p *Pool) Metrics() Snapshot {
	return p.metrics.Snapshot()
}

// Stop gracefully stops the pool: prevents new tasks, processes enqueued items, and shuts down.
func (p *Pool) Stop() {
	p.closeOnce.Do(func() {
		p.isClosed.Store(true)
		close(p.taskQueue)

		// Wait for active workers to drain the queue
		p.wg.Wait()

		if p.limiter != nil {
			p.limiter.Stop()
		}

		p.cancel()
		close(p.resultsQueue)
	})
}
