package orchestrator

import (
	"context"
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
	AddTask(ctx context.Context, t task.Task)
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

func (o *orchestrator) AddTask(ctx context.Context, t task.Task) {
	if err := t.Validate(); err != nil {
		o.logger.Error("Invalid task", "err", err)
		return
	}

	switch t.ExecutionMode {
	case string(task.Sequential):
		score := time.Now().UnixNano()
		err := o.redisClient.ZAdd(ctx, "sequential:"+t.Group, redis.Z{
			Score:  float64(score),
			Member: t.ID,
		}).Err()
		if err != nil {
			o.logger.Error("Failed to add task to queue", "execution_mode", string(task.Sequential), "task_id", t.ID, "err", err)
			return
		}
	case string(task.Concurrent):
		err := o.redisClient.LPush(ctx, taskQueue, t.ID).Err()
		if err != nil {
			o.logger.Error("Failed to add task to queue", "execution_mode", string(task.Concurrent), "task_id", t.ID, "err", err)
			return
		}
	default:
		o.logger.Error("Invalid execution mode", "execution_mode", t.ExecutionMode, "task_id", t.ID)
		return
	}

	o.redisClient.HSet(ctx, "taskState", t.ID, string(task.Pending))
	o.redisClient.HSet(ctx, "taskGroup", t.ID, t.Group)
	o.redisClient.HSet(ctx, "taskExecutionMode", t.ID, string(t.ExecutionMode))
}

func (o *orchestrator) HandleTasks(ctx context.Context) {
	for {
		// Process concurrent tasks
		result, err := o.redisClient.BRPop(ctx, 0, taskQueue).Result()
		if err != nil {
			o.logger.Error("Error fetching task from concurrent queue", "err", err)
			continue
		}

		taskID := result[1]
		executionMode, err := o.redisClient.HGet(ctx, "taskExecutionMode", taskID).Result()
		if err != nil {
			o.logger.Error("Error fetching execution mode for task", "task_id", taskID, "err", err)
			continue
		}
		o.logger.Info("Processing concurrent task", "execution_mode", executionMode, "task_id", taskID)

		// Execute concurrent task
		go o.executeTask(ctx, taskID, "", o.config.SimulatedExecutionTime)

		// Process sequential tasks
		groups, err := o.redisClient.Keys(ctx, "sequential:*").Result()
		if err != nil {
			o.logger.Error("Error fetching sequential groups", "err", err)
			continue
		}

		for _, groupKey := range groups {
			// Acquire lock for the group
			if !o.acquireGroupLock(ctx, groupKey) {
				o.logger.Debug("Lock not acquired for sequential group", "group", groupKey)
				continue
			}
			o.logger.Info("Lock acquired for sequential group", "group", groupKey)

			// Fetch the next task
			taskResult, err := o.redisClient.ZPopMin(ctx, groupKey).Result()
			if err != nil || len(taskResult) == 0 {
				o.logger.Info("No task in sequential group", "group", groupKey)
				o.releaseGroupLock(ctx, groupKey)
				continue
			}

			taskID := taskResult[0].Member.(string)
			o.logger.Info("Processing sequential task", "task_id", taskID, "group", groupKey)

			// Execute task
			o.executeTask(ctx, taskID, groupKey, o.config.SimulatedExecutionTime)

			// Release lock
			o.releaseGroupLock(ctx, groupKey)
			o.logger.Info("Lock released for sequential group", "group", groupKey)
		}
	}
}

func (o *orchestrator) acquireGroupLock(ctx context.Context, group string) bool {
	locked := o.redisClient.SetNX(ctx, "groupLock:"+group, "locked", o.heartbeatTTL).Val()
	o.logger.Debug("Attempting to acquire group lock", "group", group, "locked", locked)
	return locked
}

func (o *orchestrator) releaseGroupLock(ctx context.Context, group string) {
	o.redisClient.Del(ctx, "groupLock:"+group)
}

func (o *orchestrator) executeTask(ctx context.Context, taskID, group string, simulatedExecutionTime int) {
	retryKey := "taskRetries"

	// Fetch current retry count
	retryCountStr, err := o.redisClient.HGet(ctx, retryKey, taskID).Result()
	retryCount := 0
	if err == nil {
		retryCount, _ = strconv.Atoi(retryCountStr)
	} else if err != redis.Nil {
		o.logger.Error("Failed to fetch retry count", "task_id", taskID, "err", err)
		return
	}

	// Check if retry count exceeds the limit
	if retryCount >= retryLimit {
		o.logger.Warn("Task exceeded retry limit", "retry_count", retryCount, "task_id", taskID)
		// Mark task as Failed
		o.redisClient.HSet(ctx, "taskState", taskID, string(task.Failed))
		return
	}

	// Mark task as Running
	o.redisClient.HSet(ctx, "taskState", taskID, string(task.Running))

	// Simulate task execution
	success := task.DefaultExecute(taskID, o.logger, simulatedExecutionTime)
	if success {
		o.logger.Info("Task completed successfully", "task_id", taskID)
		// Mark task as Success and reset retry count
		o.redisClient.HSet(ctx, "taskState", taskID, string(task.Success))
		o.redisClient.HDel(ctx, retryKey, taskID)
	} else {
		// Increment retry count and retry the task
		retryCount++
		o.redisClient.HSet(ctx, retryKey, taskID, retryCount)

		backoff := time.Duration(math.Pow(2, float64(retryCount))) * time.Second
		o.logger.Warn("Retrying task", "task_id", taskID, "retry_count", retryCount, "backoff", backoff)
		time.Sleep(backoff)

		// Requeue task
		if group != "" {
			o.redisClient.ZAdd(ctx, "sequential:"+group, redis.Z{
				Score:  float64(time.Now().UnixNano()),
				Member: taskID,
			})
		} else {
			o.redisClient.LPush(ctx, taskQueue, taskID)
		}
		o.redisClient.HSet(ctx, "taskState", taskID, string(task.Pending))
	}
}

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
