package worker

import (
	"context"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
	"time"
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
		select {
		case <-ctx.Done():
			w.logger.Info("Worker stopped due to context cancellation", "worker_id", w.ID)
			return
		default:
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
}

func (w *Worker) sendHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(w.heartbeatTTL / 2) // Use half the TTL for regular updates
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Stopping heartbeat due to context cancellation", "worker_id", w.ID)
			return
		case <-ticker.C:
			// Use SET with expiration
			err := w.redisClient.Set(ctx, "workerStatus:"+w.ID, "active", w.heartbeatTTL).Err()
			if err != nil {
				w.logger.Error("Failed to send worker heartbeat", "worker_id", w.ID, "err", err)
			} else {
				w.logger.Debug("Heartbeat sent", "worker_id", w.ID)
			}
		}
	}
}

// executeTask simulates task execution.
func (w *Worker) executeTask(ctx context.Context, taskID string) bool {
	w.logger.Info("Executing task", "task_id", taskID)
	time.Sleep(2 * time.Second)     // Simulated delay
	return time.Now().Unix()%2 == 0 // Simulated random success/failure
}
