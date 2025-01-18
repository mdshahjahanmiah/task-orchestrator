package worker

import (
	"context"
	"github.com/mdshahjahanmiah/explore-go/logging"
	"time"

	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
)

type Worker struct {
	ID           string
	heartbeatTTL time.Duration
	redisClient  *redis.Client
	logger       *logging.Logger
}

// NewWorker initializes a new worker with a unique ID.
func NewWorker(id string, redisClient *redis.Client, logger *logging.Logger, heartbeatTTL time.Duration) *Worker {
	return &Worker{
		ID:           id,
		redisClient:  redisClient,
		logger:       logger,
		heartbeatTTL: heartbeatTTL,
	}
}

func (w *Worker) Start(ctx context.Context) {
	go w.sendHeartbeat(ctx)

	for {
		// Pull task from the queue
		result, err := w.redisClient.BRPop(ctx, 0, "taskQueue").Result()
		if err != nil {
			w.logger.Error("Error fetching task", "err", err)
			continue
		}

		if len(result) < 2 {
			w.logger.Info("Unexpected response from BRPop", "result", result)
			continue
		}

		taskID := result[1]
		w.logger.Info("Pulled task", "task_id", taskID)

		// Mark task as running
		w.redisClient.HSet(ctx, "taskState", taskID, string(task.Running))

		// Execute the task
		success := w.executeTask(ctx, taskID)

		// Report the result to the orchestrator
		if success {
			w.redisClient.HSet(ctx, "taskState", taskID, string(task.Success))
			w.logger.Info("Task completed successfully", "task_id", taskID)
		} else {
			w.redisClient.HSet(ctx, "taskState", taskID, string(task.Failed))
			w.logger.Error("Task failed", "task_id", taskID)
		}
	}
}

func (w *Worker) sendHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(w.heartbeatTTL / 2) // Use worker's heartbeatTTL
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.redisClient.HSet(ctx, "workerStatus", w.ID, "active")
		}
	}
}

// executeTask simulates task execution.
func (w *Worker) executeTask(ctx context.Context, taskID string) bool {
	w.logger.Info("Executing task", "task_id", taskID)
	time.Sleep(2 * time.Second)     // Simulated delay
	return time.Now().Unix()%2 == 0 // Simulated random success/failure
}
