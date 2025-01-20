package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	redisClient "github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
	"github.com/redis/go-redis/v9"
	"math"
	"strconv"
	"time"
)

const (
	taskQueue    = "taskQueue"
	retryLimit   = 3
	heartbeatTTL = 10 * time.Second
)

type Orchestrator interface {
	AddTask(ctx context.Context, t task.Task) error
	HandleTasks(ctx context.Context)
	MonitorWorkers(ctx context.Context)
	ReassignTasks(ctx context.Context, worker string)
}

type orchestrator struct {
	heartbeatTTL time.Duration
	retries      map[string]int
	config       config.Config
	redisClient  *redisClient.Client
	logger       *logging.Logger
}

func NewOrchestrator(config config.Config, redisClient *redisClient.Client, logger *logging.Logger) Orchestrator {
	return &orchestrator{
		config:       config,
		redisClient:  redisClient,
		logger:       logger,
		retries:      make(map[string]int),
		heartbeatTTL: heartbeatTTL,
	}
}

// AddTask adds a task to the appropriate queue based on its execution mode.
func (o *orchestrator) AddTask(ctx context.Context, t task.Task) error {
	if err := t.Validate(); err != nil {
		o.logger.Error("Invalid task", "err", err)
		return err
	}

	switch t.ExecutionMode {
	case string(task.Sequential):
		o.addSequentialTask(ctx, t)
	case string(task.Concurrent):
		o.addConcurrentTask(ctx, t)
	default:
		err := fmt.Errorf("invalid execution mode: %s", t.ExecutionMode)
		o.logger.Error("Invalid execution mode", "execution_mode", t.ExecutionMode, "task_id", t.ID)
		return err
	}
	return nil
}

// HandleTasks processes tasks by delegating to concurrent and sequential task handlers.
// It runs continuously until the context is canceled.
func (o *orchestrator) HandleTasks(ctx context.Context) {
	o.logger.Debug("Starting to handle tasks...")
	for {
		select {
		case <-ctx.Done():
			o.logger.Info("Stopping task processing due to context cancellation")
			return
		default:
			o.logger.Debug("Fetching concurrent tasks...")
			o.processConcurrentTasks(ctx)

			o.logger.Debug("Fetching sequential tasks...")
			o.processSequentialTasks(ctx)
		}
	}
}

// MonitorWorkers periodically checks the status of workers and reassigns tasks
// for unresponsive workers. It runs until the context is canceled.
func (o *orchestrator) MonitorWorkers(ctx context.Context) {
	ticker := time.NewTicker(o.heartbeatTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			workers, err := o.redisClient.HGetAll(ctx, "workerStatus").Result()
			if err != nil {
				o.logger.Error("Error fetching worker status", "err", err)
				continue
			}

			for workerID, status := range workers {
				if status != "active" {
					o.logger.Warn("Worker unresponsive, reassigning tasks", "worker_id", workerID)
					o.ReassignTasks(ctx, workerID)
					o.redisClient.HDel(ctx, "workerStatus", workerID)
				}
			}
		}
	}
}

// ReassignTasks redistributes tasks from an unresponsive worker back to the task queue
// and updates their state to pending.
func (o *orchestrator) ReassignTasks(ctx context.Context, worker string) {
	tasks, err := o.redisClient.HGetAll(ctx, "workerTasks:"+worker).Result()
	if err != nil {
		o.logger.Error("Error fetching tasks for unresponsive worker", "worker_id", worker, "err", err)
		return
	}

	for taskID := range tasks {
		o.logger.Info("Reassigning task", "task_id", taskID)
		o.redisClient.LPush(ctx, taskQueue, taskID)
		o.redisClient.HSet(ctx, "taskState", taskID, string(task.Pending))
	}
	o.redisClient.Del(ctx, "workerTasks:"+worker)
}

// addConcurrentTask enqueues a task in the concurrent task queue and updates its state to pending.
func (o *orchestrator) addConcurrentTask(ctx context.Context, t task.Task) {
	if err := o.redisClient.LPush(ctx, taskQueue, t.ID).Err(); err != nil {
		o.logger.Error("Failed to add task to concurrent queue", "task_id", t.ID, "err", err)
		return
	}
	o.logTaskState(ctx, t, task.Pending)
}

// addSequentialTask enqueues a task in the sequential task queue,
// using a timestamp as the score to maintain execution order, and updates its state to pending.
func (o *orchestrator) addSequentialTask(ctx context.Context, t task.Task) error {
	score := time.Now().UnixNano()
	if err := o.redisClient.ZAdd(ctx, "sequential:"+t.Group, redis.Z{
		Score:  float64(score),
		Member: t.ID,
	}).Err(); err != nil {
		o.logger.Error("Failed to add task to sequential queue", "task_id", t.ID, "err", err)
		return err
	}
	o.logTaskState(ctx, t, task.Pending)

	return nil
}

// logTaskState updates the task's metadata in Redis, including its state, group, and execution mode.
func (o *orchestrator) logTaskState(ctx context.Context, t task.Task, state task.State) {
	o.redisClient.HSet(ctx, "taskState", t.ID, string(state))
	o.redisClient.HSet(ctx, "taskGroup", t.ID, t.Group)
	o.redisClient.HSet(ctx, "taskExecutionMode", t.ID, string(t.ExecutionMode))
}

// processConcurrentTasks retrieves and processes tasks from the concurrent task queue.
// Each task is executed asynchronously in a separate goroutine.
func (o *orchestrator) processConcurrentTasks(ctx context.Context) {
	result, err := o.redisClient.BRPop(ctx, 0, taskQueue).Result()
	if err != nil {
		o.logger.Error("Error fetching task from concurrent queue", "err", err)
		return
	}

	taskID := result[1]
	o.logger.Info("Processing concurrent task", "task_id", taskID)

	go o.executeTask(ctx, taskID, "", o.config.SimulatedExecutionTime)
}

// processSequentialTasks processes tasks from sequential task groups in order.
// It acquires a lock for each group to ensure only one task is processed at a time,
// and releases the lock after processing.
func (o *orchestrator) processSequentialTasks(ctx context.Context) {
	groups, err := o.redisClient.Keys(ctx, "sequential:*").Result()
	if err != nil {
		o.logger.Error("Error fetching sequential groups", "err", err)
		return
	}

	for _, groupKey := range groups {
		if !o.acquireGroupLock(ctx, groupKey) {
			o.logger.Debug("Lock not acquired for sequential group", "group", groupKey)
			continue
		}
		o.logger.Info("Lock acquired for sequential group", "group", groupKey)

		taskResult, err := o.redisClient.ZPopMin(ctx, groupKey).Result()
		if err != nil || len(taskResult) == 0 {
			o.logger.Info("No task in sequential group", "group", groupKey)
			o.releaseGroupLock(ctx, groupKey)
			continue
		}

		taskID := taskResult[0].Member.(string)
		o.logger.Info("Processing sequential task", "task_id", taskID, "group", groupKey)

		o.executeTask(ctx, taskID, groupKey, o.config.SimulatedExecutionTime)
		o.releaseGroupLock(ctx, groupKey)
		o.logger.Info("Lock released for sequential group", "group", groupKey)
	}
}

// acquireGroupLock attempts to acquire a lock for the specified group to ensure
// exclusive task processing. Returns true if the lock is successfully acquired.
func (o *orchestrator) acquireGroupLock(ctx context.Context, group string) bool {
	locked := o.redisClient.SetNX(ctx, "groupLock:"+group, "locked", o.heartbeatTTL).Val()
	o.logger.Debug("Attempting to acquire group lock", "group", group, "locked", locked)
	return locked
}

// releaseGroupLock releases the lock for the specified group, allowing other processes to acquire it.
func (o *orchestrator) releaseGroupLock(ctx context.Context, group string) {
	o.redisClient.Del(ctx, "groupLock:"+group)
}

// executeTask handles the execution of a single task. It manages task retries,
// updates the task state, and simulates execution. If the retry limit is reached,
// the task is marked as failed.
func (o *orchestrator) executeTask(ctx context.Context, taskID, group string, simulatedExecutionTime int) {
	retryKey := "taskRetries"
	retryCount := o.getRetryCount(ctx, retryKey, taskID)

	// Check if retry count exceeds the limit
	//TODO: use retryLimit from task config
	if retryCount >= retryLimit {
		o.markTaskFailed(ctx, taskID, retryCount)
		return
	}

	// Mark task as Running
	o.markTaskRunning(ctx, taskID)

	// Simulate task execution
	success := task.DefaultExecute(taskID, o.logger, simulatedExecutionTime)

	if success {
		// Task completed successfully
		o.markTaskSuccess(ctx, taskID)
	} else {
		// Handle task retry
		o.handleRetry(ctx, taskID, group, retryCount, retryKey)
	}
}

// getRetryCount retrieves the current retry count for a given task from Redis.
// If the retry count is unavailable or an error occurs, it defaults to 0.
func (o *orchestrator) getRetryCount(ctx context.Context, retryKey, taskID string) int {
	retryCountStr, err := o.redisClient.HGet(ctx, retryKey, taskID).Result()
	if err == nil {
		retryCount, _ := strconv.Atoi(retryCountStr)
		return retryCount
	} else if !errors.Is(redis.Nil, err) {
		o.logger.Error("Failed to fetch retry count", "task_id", taskID, "err", err)
	}
	return 0
}

// markTaskFailed marks a task as failed in Redis and logs a warning when the retry limit is exceeded.
func (o *orchestrator) markTaskFailed(ctx context.Context, taskID string, retryCount int) {
	o.logger.Warn("Task exceeded retry limit", "retry_count", retryCount, "task_id", taskID)
	o.redisClient.HSet(ctx, "taskState", taskID, string(task.Failed))
}

// markTaskRunning updates the task's state to running in Redis.
func (o *orchestrator) markTaskRunning(ctx context.Context, taskID string) {
	o.redisClient.HSet(ctx, "taskState", taskID, string(task.Running))
}

// markTaskSuccess updates the task's state to success in Redis,
// logs the completion, and clears the retry count for the task.
func (o *orchestrator) markTaskSuccess(ctx context.Context, taskID string) {
	o.logger.Info("Task completed successfully", "task_id", taskID)
	o.redisClient.HSet(ctx, "taskState", taskID, string(task.Success))
	o.redisClient.HDel(ctx, "taskRetries", taskID)
}

// handleRetry increments the retry count for a task, calculates an exponential backoff,
// and requeues the task for processing based on its group (sequential or concurrent).
func (o *orchestrator) handleRetry(ctx context.Context, taskID, group string, retryCount int, retryKey string) {
	// Increment retry count
	retryCount++
	o.redisClient.HSet(ctx, retryKey, taskID, retryCount)

	// Calculate backoff duration
	backoff := time.Duration(math.Pow(2, float64(retryCount))) * time.Second
	o.logger.Warn("Retrying task", "task_id", taskID, "retry_count", retryCount, "backoff", backoff)

	// Wait for backoff and requeue the task
	time.AfterFunc(backoff, func() {
		if group != "" {
			// Requeue for sequential processing
			o.redisClient.ZAdd(ctx, "sequential:"+group, redis.Z{
				Score:  float64(time.Now().UnixNano()),
				Member: taskID,
			})
		} else {
			// Requeue for concurrent processing
			o.redisClient.LPush(ctx, taskQueue, taskID)
		}
		o.redisClient.HSet(ctx, "taskState", taskID, string(task.Pending))
	})
}
