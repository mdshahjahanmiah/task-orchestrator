package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/orchestrator"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/worker"
)

func main() {
	conf, err := config.Load()
	if err != nil {
		log.Printf("Failed to load configuration: %v\n", err)
		return
	}
	log.Println("Configuration loaded successfully")

	logger, err := logging.NewLogger(conf.LoggerConfig)
	if err != nil {
		log.Printf("Failed to initialize logger: %v\n", err)
		return
	}
	log.Println("Logger initialized successfully")

	redisClient := redis.NewClient(conf)
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		logger.Fatal("Failed to connect to Redis", "err", err)
	}
	orchestrator := orchestrator.NewOrchestrator(conf, redisClient, logger)
	if orchestrator == nil {
		logger.Fatal("Failed to initialize orchestrator")
	}

	// Submit 10 tasks
	submitTestTasks(orchestrator, logger)

	// Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handling for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		log.Println("Received shutdown signal. Cleaning up...")
		cancel()
	}()

	// Start orchestrator
	go orchestrator.HandleTasks(ctx)
	go orchestrator.MonitorWorkers(ctx)

	// Start workers
	startWorkers(ctx, redisClient, logger, 3) // Start 3 workers

	<-ctx.Done()
	if err := redisClient.Close(); err != nil {
		logger.Fatal("Failed to shut down Redis client", "err", err)
	}
	logger.Info("Service shutdown completed")
}

// submitTestTasks submits sample tasks to the orchestrator.
func submitTestTasks(orchestrator orchestrator.Orchestrator, logger *logging.Logger) {
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

// startWorkers initializes and starts multiple workers.
func startWorkers(ctx context.Context, redisClient *redis.Client, logger *logging.Logger, workerCount int) {
	for i := 1; i <= workerCount; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		w := worker.NewWorker(workerID, redisClient, logger, 10*time.Second)
		go w.Start(ctx)
		logger.Info("Started worker", "worker_id", workerID)
	}
}
