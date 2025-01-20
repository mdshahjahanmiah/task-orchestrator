package utils

import (
	"context"
	"fmt"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	logging "github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/orchestrator"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/worker"
	"time"
)

// SubmitTestTasks submits sample tasks to the orchestrator.
func SubmitTestTasks(orchestrator orchestrator.Orchestrator, config *config.Config, logger *logging.Logger) {
	ctx := context.Background()

	for i := 1; i <= 10; i++ {
		executionMode := string(task.Concurrent)
		group := "group1"

		if i%2 == 0 {
			executionMode = string(task.Sequential)
			group = "group2"
		}

		task := task.Task{
			ID:            fmt.Sprintf("task-%d", i),
			ExecutionMode: executionMode,
			Group:         group,
			Payload: task.Payload{
				Data:     fmt.Sprintf("Task Data %d", i),
				Duration: config.SimulatedExecutionTime,
			},
		}

		orchestrator.AddTask(ctx, task)
		logger.Info("Submitted task", "mode", task.ExecutionMode, "group", task.Group, "task_id", task.ID)
	}
}

// StartWorkers initializes and starts multiple workers.
func StartWorkers(ctx context.Context, redisClient *redis.Client, config *config.Config, logger *logging.Logger, workerCount int) {
	for i := 1; i <= workerCount; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		w := worker.NewWorker(workerID, redisClient, config, logger, 10*time.Second)
		go w.Start(ctx)
		logger.Debug("Started worker", "worker_id", workerID)
	}
}

// ClearRedisKeys deletes the specified keys from Redis.
func ClearRedisKeys(logger *logging.Logger, redisClient *redis.Client, keys ...string) {
	_, err := redisClient.Del(context.Background(), keys...).Result()
	if err != nil {
		logger.Error("Failed to clear Redis keys", "err", err)
	} else {
		logger.Info("Redis keys cleared", "keys", keys)
	}
}
