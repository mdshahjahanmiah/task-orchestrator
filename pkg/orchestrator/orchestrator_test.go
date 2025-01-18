package orchestrator

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
	"github.com/stretchr/testify/assert"
	"testing"
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

func Test_AddTask_Sequential(t *testing.T) {
	o, mr, ctx := setupOrchestratorTest(t)
	defer mr.Close()

	task := task.Task{
		ID:            "task-seq",
		ExecutionMode: string(task.Sequential),
		Group:         "group-seq",
		Payload: task.Payload{
			Data:     "Sequential Task Data",
			Duration: 2,
		},
	}

	o.AddTask(ctx, task)

	// Assert task is added to sequential group
	assert.Equal(t, "Pending", mr.HGet("taskState", task.ID))
	assert.Equal(t, "sequential", mr.HGet("taskExecutionMode", task.ID))
	assert.True(t, mr.Exists("sequential:group-seq"))
}

func Test_AddTask_Concurrent(t *testing.T) {
	o, mr, ctx := setupOrchestratorTest(t)
	defer mr.Close()

	task := task.Task{
		ID:            "task-con",
		ExecutionMode: string(task.Concurrent),
		Group:         "group-con",
		Payload: task.Payload{
			Data:     "Concurrent Task Data",
			Duration: 2,
		},
	}

	o.AddTask(ctx, task)

	// Assert task is added to task queue
	assert.Equal(t, "Pending", mr.HGet("taskState", task.ID))
	assert.Equal(t, "concurrent", mr.HGet("taskExecutionMode", task.ID))
	assert.True(t, mr.Exists(taskQueue))
}

func Test_AddTask_InvalidMode(t *testing.T) {
	o, mr, ctx := setupOrchestratorTest(t)
	defer mr.Close()

	task := task.Task{
		ID:            "task-invalid",
		ExecutionMode: "invalid-mode",
		Group:         "group-invalid",
		Payload: task.Payload{
			Data:     "Invalid Task Data",
			Duration: 2,
		},
	}

	o.AddTask(ctx, task)

	// Assert task is not added to Redis
	assert.Empty(t, mr.HGet("taskState", task.ID))
	assert.False(t, mr.Exists(taskQueue))
	assert.False(t, mr.Exists("sequential:group-invalid"))
}
