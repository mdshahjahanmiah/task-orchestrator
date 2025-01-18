package task

import (
	"errors"
	logging "github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"time"
)

type State string

const (
	Pending State = "Pending"
	Running State = "Running"
	Success State = "Success"
	Failed  State = "Failed"
)

type ExecutionMode string

const (
	Concurrent ExecutionMode = "concurrent"
	Sequential ExecutionMode = "sequential"
)

type Payload struct {
	Data     string `json:"data"`
	Duration int    `json:"duration"`
}

type Task struct {
	ID            string  `json:"task_id"`
	ExecutionMode string  `json:"execution_mode"`
	Group         string  `json:"task_group"`
	Payload       Payload `json:"payload"`
}

var DefaultExecute = func(taskID string, logger *logging.Logger) bool {
	logger.Info("Executing task", "task_id", taskID)
	time.Sleep(2 * time.Second) // Simulated task duration
	return time.Now().Unix()%2 == 0
}

func (t *Task) Validate() error {
	if t.ID == "" || t.Group == "" || (t.ExecutionMode != string(Concurrent) && t.ExecutionMode != string(Sequential)) {
		return errors.New("invalid task: missing required fields or invalid execution mode")
	}
	return nil
}

func Execute(taskID string, logger *logging.Logger) bool {
	return DefaultExecute(taskID, logger)
}
