package main

import (
	"context"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/orchestrator"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/utils"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	conf, err := config.Load()
	if err != nil {
		slog.Info("Failed to load configuration", "err", err)
		return
	}
	slog.Info("Configuration loaded successfully")

	logger, err := logging.NewLogger(conf.LoggerConfig)
	if err != nil {
		slog.Info("Failed to initialize logger", "err", err)
		return
	}
	logger.Info("Logger initialized successfully")

	redisClient := redis.NewClient(conf)
	if _, err := redisClient.Ping(context.Background()).Result(); err != nil {
		logger.Fatal("Failed to connect to Redis", "err", err)
	}
	orchestrator := orchestrator.NewOrchestrator(conf, redisClient, logger)
	if orchestrator == nil {
		logger.Fatal("Failed to initialize orchestrator")
	}

	// Clear Redis keys for a clean state
	utils.ClearRedisKeys(redisClient, "taskState", "taskRetries")
	logger.Info("Redis keys cleared")

	// Submit 10 tasks
	utils.SubmitTestTasks(orchestrator, logger)

	// Setup context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Signal handling for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		logger.Info("Received shutdown signal. Cleaning up...")
		cancel()
	}()

	// Start orchestrator
	go orchestrator.HandleTasks(ctx)
	go orchestrator.MonitorWorkers(ctx)

	// Start workers
	utils.StartWorkers(ctx, redisClient, &conf, logger, conf.WorkerCount) // Start 3 workers

	<-ctx.Done()
	if err := redisClient.Close(); err != nil {
		logger.Fatal("Failed to shut down Redis client", "err", err)
	}
	logger.Info("Service shutdown completed")
}
