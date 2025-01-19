package worker

import (
	"context"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
	"math/rand"
	"time"
)

type Worker struct {
	ID           string
	heartbeatTTL time.Duration
	redisClient  *redis.Client
	config       *config.Config
	logger       *logging.Logger
}

// NewWorker initializes a new worker with a unique ID.
func NewWorker(id string, redisClient *redis.Client, config *config.Config, logger *logging.Logger, heartbeatTTL time.Duration) *Worker {
	return &Worker{
		ID:           id,
		redisClient:  redisClient,
		config:       config,
		logger:       logger,
		heartbeatTTL: heartbeatTTL,
	}
}

func (w *Worker) Start(ctx context.Context) {
	w.logger.Info("Worker started", "worker_id", w.ID)
	go w.sendHeartbeat(ctx)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Worker stopped due to context cancellation", "worker_id", w.ID)
			return
		default:
			w.logger.Debug("Worker waiting for task...", "worker_id", w.ID)

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
			success := w.executeTask(ctx, taskID, w.config.SimulatedExecutionTime)

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

func (w *Worker) executeTask(ctx context.Context, taskID string, duration int) bool {
	w.logger.Info("Executing task", "task_id", taskID, "duration", duration)

	select {
	case <-time.After(time.Duration(duration) * time.Second): // Simulates task execution
		return w.simulateTaskOutcome(taskID)
	case <-ctx.Done():
		// Context canceled, exit early
		w.logger.Warn("Task execution canceled", "task_id", taskID, "reason", ctx.Err())
		return false
	}
}

func (w *Worker) simulateTaskOutcome(taskID string) bool {
	successRate := w.config.SuccessRate // Configurable success rate
	randomValue := rand.Intn(100)       // Random number between 0-99
	w.logger.Debug("Task execution outcome", "task_id", taskID, "random_value", randomValue, "success_rate", successRate)
	return randomValue < successRate
}
