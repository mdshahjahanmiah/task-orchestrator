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
	"log"
	"time"
)

// SubmitTestTasks submits sample tasks to the orchestrator.
func SubmitTestTasks(orchestrator orchestrator.Orchestrator, logger *logging.Logger) {
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
				Duration: 2,
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
		logger.Info("Started worker", "worker_id", workerID)
	}
}

func ClearRedisKeys(redisClient *redis.Client, keys ...string) {
	_, err := redisClient.Del(context.Background(), keys...).Result()
	if err != nil {
		log.Printf("Failed to clear Redis keys: %v\n", err)
	} else {
		log.Println("Redis keys cleared:", keys)
	}
}
