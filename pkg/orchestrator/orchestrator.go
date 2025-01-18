package orchestrator

import (
	"context"
	"github.com/mdshahjahanmiah/explore-go/logging"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/config"
	redisClient "github.com/mdshahjahanmiah/task-orchestrator/pkg/redis"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/task"
	"github.com/redis/go-redis/v9"
	"math"
	"sync"
	"time"
)

const (
	taskQueue       = "taskQueue"
	workerHeartbeat = "workerHeartbeat"
	retryLimit      = 3
	heartbeatTTL    = 10 * time.Second
)

type Orchestrator interface {
	AddTask(ctx context.Context, t task.Task)
	HandleTasks(ctx context.Context)
	MonitorWorkers(ctx context.Context)
	ReassignTasks(ctx context.Context, worker string)
}

type orchestrator struct {
	mu           sync.Mutex
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

	o.logger.Debug("Adding task", "task_id", t.ID)

	switch t.ExecutionMode {
	case string(task.Sequential):
		// Add sequential tasks to a Redis sorted set
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
		// Push concurrent tasks directly to the task queue
		err := o.redisClient.LPush(ctx, taskQueue, t.ID).Err()
		if err != nil {
			o.logger.Error("Failed to add task to queue", "execution_mode", string(task.Concurrent), "task_id", t.ID, "err", err)
			return
		}
	default:
		// Invalid execution mode
		o.logger.Error("Invalid execution mode", "execution_mode", t.ExecutionMode, "task_id", t.ID)
		return
	}

	// Track task metadata in Redis
	o.redisClient.HSet(ctx, "taskState", t.ID, string(task.Pending))
	o.redisClient.HSet(ctx, "taskGroup", t.ID, t.Group)
	o.redisClient.HSet(ctx, "taskExecutionMode", t.ID, string(t.ExecutionMode))
}

func (o *orchestrator) HandleTasks(ctx context.Context) {
	for {
		// Process concurrent tasks from the task queue
		result, err := o.redisClient.BRPop(ctx, 0, taskQueue).Result()
		if err != nil {
			o.logger.Error("Error fetching task", "err", err)
			continue
		}

		taskID := result[1]
		o.logger.Info("Processing task", "task_id", taskID)

		// Execute the task
		o.executeTask(ctx, taskID, "")

		// Process sequential tasks for all groups
		groups, err := o.redisClient.Keys(ctx, "sequential:*").Result()
		if err != nil {
			o.logger.Error("Error fetching sequential groups", "err", err)
			continue
		}

		for _, groupKey := range groups {
			if o.acquireGroupLock(ctx, groupKey) {
				taskID, err := o.redisClient.ZPopMin(ctx, groupKey).Result()
				if err != nil || len(taskID) == 0 {
					o.logger.Info("No task in sequential group", "group", groupKey)
					o.releaseGroupLock(ctx, groupKey)
					continue
				}
				o.logger.Info("Processing sequential task", "task_id", taskID)
				o.executeTask(ctx, taskID[0].Member.(string), groupKey)
				o.releaseGroupLock(ctx, groupKey)
			}
		}
	}
}

func (o *orchestrator) acquireGroupLock(ctx context.Context, group string) bool {
	return o.redisClient.SetNX(ctx, "groupLock:"+group, "locked", o.heartbeatTTL).Val()
}

func (o *orchestrator) releaseGroupLock(ctx context.Context, group string) {
	o.redisClient.Del(ctx, "groupLock:"+group)
}

func (o *orchestrator) executeTask(ctx context.Context, taskID, group string) {
	retryKey := "retryCount:" + taskID
	retryCount, _ := o.redisClient.Get(ctx, retryKey).Int()

	if retryCount >= retryLimit {
		o.logger.Warn("Task exceeded retry limit", "task_id", taskID)
		o.redisClient.HSet(ctx, "taskState", taskID, string(task.Failed))
		return
	}

	// Mark task as running
	o.redisClient.HSet(ctx, "taskState", taskID, string(task.Running))

	if success := task.Execute(taskID); success {
		o.redisClient.HSet(ctx, "taskState", taskID, string(task.Success))
		o.redisClient.Del(ctx, retryKey) // Clear retry count on success
	} else {
		retryCount++
		backoff := time.Duration(math.Pow(2, float64(retryCount))) * time.Second
		o.logger.Warn("Retrying task", "task_id", taskID, "retry_count", retryCount, "backoff", backoff)
		time.Sleep(backoff)

		// Update retry count and requeue task
		o.redisClient.Set(ctx, retryKey, retryCount, 0)
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
	for {
		workers, err := o.redisClient.Keys(ctx, "worker:*").Result()
		if err != nil {
			o.logger.Error("Error fetching workers", "err", err)
			continue
		}

		for _, worker := range workers {
			exists, err := o.redisClient.Expire(ctx, worker, o.heartbeatTTL).Result()
			if err != nil || !exists {
				o.logger.Warn("Worker is unresponsive; reassigning tasks", "worker", worker)
				o.ReassignTasks(ctx, worker)
			}
		}

		time.Sleep(o.heartbeatTTL / 2)
	}
}

func (o *orchestrator) ReassignTasks(ctx context.Context, worker string) {
	tasks, err := o.redisClient.HGetAll(ctx, "workerTasks:"+worker).Result()
	if err != nil {
		o.logger.Error("Error fetching tasks for worker", "worker", worker, "err", err)
		return
	}

	for taskID := range tasks {
		o.logger.Info("Reassigning task", "task_id", taskID)
		o.redisClient.LPush(ctx, taskQueue, taskID)
		o.redisClient.HSet(ctx, "taskState", taskID, string(task.Pending))
	}
	o.redisClient.Del(ctx, "workerTasks:"+worker)
}
