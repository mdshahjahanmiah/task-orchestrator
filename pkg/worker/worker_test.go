package worker

import (
	"context"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/stretchr/testify/assert"
)

func setupWorkerTest(t *testing.T) (*Worker, *miniredis.Miniredis, context.Context) {
	// Start a mock Redis server
	mr, err := miniredis.Run()
	assert.NoError(t, err)

	config := config.Config{
		RedisAddress: mr.Addr(),
	}

	// Configure mock Redis client
	redisClient := redis.NewClient(config)

	// Create a logger
	logger, err := logging.NewLogger(logging.LoggerConfig{
		LogLevel:       "DEBUG",
		CommandHandler: "text",
		AddSource:      false,
	})
	assert.NoError(t, err)

	// Initialize the worker
	worker := NewWorker("worker-1", redisClient, &config, logger, 5*time.Second)

	// Return worker, mock Redis, and context
	return worker, mr, context.Background()
}

func Test_WorkerStartAndHeartbeat(t *testing.T) {
	w, mr, ctx := setupWorkerTest(t)
	defer mr.Close()

	// Start worker in a separate goroutine
	go w.Start(ctx)

	// Allow heartbeat to send at least once
	time.Sleep(3 * time.Second)

	// Verify heartbeat in Redis
	heartbeat, _ := mr.Get("workerStatus:worker-1")
	assert.Equal(t, "active", heartbeat, "Worker heartbeat not updated in Redis")
}
