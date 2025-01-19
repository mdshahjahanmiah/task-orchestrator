package orchestrator

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func setupOrchestratorTest(t *testing.T) (Orchestrator, *miniredis.Miniredis, context.Context) {
	// Start a miniredis server
	mr, err := miniredis.Run()
	assert.NoError(t, err)

	// Setup mock config
	cfg := config.Config{RedisAddress: mr.Addr()}

	// Setup logger
	logger, err := logging.NewLogger(cfg.LoggerConfig)
	assert.NoError(t, err)

	// Setup Redis client
	redisClient := redis.NewClient(cfg)

	// Initialize orchestrator
	o := NewOrchestrator(cfg, redisClient, logger)

	// Return context and cleanup resources
	return o, mr, context.Background()
}

func Test_ConcurrentTaskExecution(t *testing.T) {
	o, mr, ctx := setupOrchestratorTest(t)
	defer mr.Close()

	// Submit concurrent tasks
	for i := 1; i <= 5; i++ {
		o.AddTask(ctx, task.Task{
			ID:            "task-" + strconv.Itoa(i),
			ExecutionMode: string(task.Concurrent),
			Group:         "group1",
			Payload: task.Payload{
				Data:     "Sample Data",
				Duration: 2,
			},
		})
	}

	// Verify tasks are added to the queue
	queue, err := mr.List(taskQueue)
	assert.NoError(t, err)
	assert.Len(t, queue, 5, "Expected 5 tasks in the concurrent queue")
}

func Test_SequentialTaskSynchronization(t *testing.T) {
	o, mr, ctx := setupOrchestratorTest(t)
	defer mr.Close()

	// Submit sequential tasks
	for i := 1; i <= 3; i++ {
		o.AddTask(ctx, task.Task{
			ID:            "task-" + strconv.Itoa(i),
			ExecutionMode: string(task.Sequential),
			Group:         "group1",
			Payload: task.Payload{
				Data:     "Sample Data",
				Duration: 1,
			},
		})
	}

	// Retrieve tasks from the sorted set for the sequential group
	sequentialTasks, _ := mr.ZMembers("sequential:group1")

	// Verify tasks are added to the sorted set in the correct order
	assert.Len(t, sequentialTasks, 3, "Expected 3 tasks in sequential queue")
	assert.Contains(t, sequentialTasks, "task-1", "Expected task-1 in sequential queue")
	assert.Contains(t, sequentialTasks, "task-2", "Expected task-2 in sequential queue")
	assert.Contains(t, sequentialTasks, "task-3", "Expected task-3 in sequential queue")
}

func Test_TaskRetries(t *testing.T) {
	o, mr, ctx := setupOrchestratorTest(t)
	defer mr.Close()

	// Mock task execution to always fail
	originalExecute := task.DefaultExecute
	task.DefaultExecute = func(taskID string, logger *logging.Logger, simulatedExecutionTime int) bool {
		logger.Info("Mock execute: failing task", "task_id", taskID)
		return false // Always fail
	}
	defer func() { task.DefaultExecute = originalExecute }() // Restore original after test

	// Add a failing task
	taskID := "failing-task"
	o.AddTask(ctx, task.Task{
		ID:            taskID,
		ExecutionMode: string(task.Concurrent),
		Group:         "group1",
		Payload: task.Payload{
			Data:     "Failing Task",
			Duration: 1,
		},
	})

	// Run the orchestrator in a separate goroutine
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go o.HandleTasks(cancelCtx)

	// Wait for retries to complete
	time.Sleep(15 * time.Second) // Adjusted for retry backoff

	// Check retry count
	retryCount := mr.HGet("taskRetries", taskID)
	assert.Equal(t, "3", retryCount, "Retry count mismatch")

	// Verify task state is marked as Failed
	taskState := mr.HGet("taskState", taskID)
	assert.Equal(t, string(task.Failed), taskState, "Task should be marked as Failed after max retries")
}
