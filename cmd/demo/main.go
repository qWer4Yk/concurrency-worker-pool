package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dmitriy-dev/concurrency-worker-pool/pkg/workerpool"
)

func main() {
	fmt.Println("🚀 Starting WorkerPool Demo...")
	fmt.Println("Configuration: 4 Workers, Queue Size 50, RPS Limit 20")

	pool := workerpool.New(workerpool.Config{
		Workers:   4,
		QueueSize: 50,
		RPS:       20, // 20 tasks/sec
	})

	// Intercept SIGINT / SIGTERM for graceful shutdown
	sigCtx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()

	// Consumer goroutine for results
	doneResults := make(chan struct{})
	go func() {
		defer close(doneResults)
		for res := range pool.Results() {
			if res.Err != nil {
				fmt.Printf("❌ [%s] Failed after %v: %v\n", res.TaskID, res.Duration, res.Err)
			} else {
				fmt.Printf("✅ [%s] Result: %v (took %v)\n", res.TaskID, res.Output, res.Duration)
			}
		}
	}()

	// Producer loop simulating incoming HTTP/Batch tasks
	go func() {
		for i := 1; i <= 30; i++ {
			select {
			case <-sigCtx.Done():
				fmt.Println("\n⚠️ Interrupted! Stopping task generation...")
				return
			default:
			}

			taskID := fmt.Sprintf("fetch-supplier-data-%02d", i)
			payloadID := i

			err := pool.Submit(workerpool.NewTask(taskID, func(ctx context.Context) (any, error) {
				// Simulate random network latency 30-100ms
				sleepTime := time.Duration(30+rand.Intn(70)) * time.Millisecond
				select {
				case <-time.After(sleepTime):
				case <-ctx.Done():
					return nil, ctx.Err()
				}

				// Simulate occasional failure
				if payloadID == 7 {
					return nil, fmt.Errorf("external supplier API returned HTTP 504 Gateway Timeout")
				}
				// Simulate accidental panic in bad payload
				if payloadID == 13 {
					panic("nil pointer dereference in payload parser")
				}

				return fmt.Sprintf("Processed hotel content payload #%d", payloadID), nil
			}))

			if err != nil {
				fmt.Printf("⚠️ Could not submit task %s: %v\n", taskID, err)
			}

			time.Sleep(30 * time.Millisecond)
		}

		fmt.Println("📦 Finished generating 30 tasks. Shutting down pool gracefully...")
		pool.Stop()
	}()

	// Wait for pool stop & all results drained
	<-doneResults

	stats := pool.Metrics()
	fmt.Println("\n================ POOL STATISTICS ================")
	fmt.Printf("Total Submitted: %d\n", stats.Submitted)
	fmt.Printf("Total Completed: %d\n", stats.Completed)
	fmt.Printf("Total Failed:    %d\n", stats.Failed)
	fmt.Printf("Total Panics:     %d (recovered safely)\n", stats.Panics)
	fmt.Println("================================================")
	fmt.Println("🎉 Clean exit.")
}
