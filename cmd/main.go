package main

import (
	"context"
	"fmt"
	"github.com/mdshahjahanmiah/explore-go/logging"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/orchestrator"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
	"log"
	"os"
	"os/signal"
	"syscall"
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
	orchestrator := orchestrator.NewOrchestrator(conf, redisClient, logger)

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

	// Start orchestrator and workers
	go orchestrator.HandleTasks(ctx)
	go orchestrator.MonitorWorkers(ctx)

	<-ctx.Done()
	log.Println("Service shutdown completed")
}

func submitTestTasks(orchestrator orchestrator.Orchestrator, logger *logging.Logger) {
	ctx := context.Background()

	for i := 1; i <= 10; i++ {
		executionMode := "concurrent"
		group := "group1"

		if i%2 == 0 {
			executionMode = "sequential"
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
