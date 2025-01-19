package task

import (
	logging "github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_TaskValidation(t *testing.T) {
	tests := []struct {
		name    string
		input   Task
		isValid bool
	}{
		{
			name: "Valid Concurrent Task",
			input: Task{
				ID:            "task-1",
				ExecutionMode: string(Concurrent),
				Group:         "group1",
				Payload:       Payload{Data: "Sample Data", Duration: 1},
			},
			isValid: true,
		},
		{
			name: "Valid Sequential Task",
			input: Task{
				ID:            "task-2",
				ExecutionMode: string(Sequential),
				Group:         "group2",
				Payload:       Payload{Data: "Sample Data", Duration: 2},
			},
			isValid: true,
		},
		{
			name: "Invalid Task with Missing Fields",
			input: Task{
				ID:            "",
				ExecutionMode: string(Concurrent),
				Group:         "",
				Payload:       Payload{Data: "Missing Fields", Duration: 1},
			},
			isValid: false,
		},
		{
			name: "Invalid Execution Mode",
			input: Task{
				ID:            "task-3",
				ExecutionMode: "invalid-mode",
				Group:         "group3",
				Payload:       Payload{Data: "Invalid Mode", Duration: 3},
			},
			isValid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.input.Validate()
			if tc.isValid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func Test_TaskExecution(t *testing.T) {
	// Initialize a logger
	logConfig := logging.LoggerConfig{
		LogLevel:       "INFO",
		CommandHandler: "text",
		AddSource:      false,
	}
	logger, err := logging.NewLogger(logConfig)
	assert.NoError(t, err, "Logger initialization should not fail")

	// Simulated task execution
	taskID := "test-task"
	start := time.Now()
	success := Execute(taskID, logger, 2)

	// Assert execution results
	duration := time.Since(start)
	assert.True(t, duration >= 2*time.Second, "Execution should take at least 2 seconds")
	assert.Contains(t, []bool{true, false}, success, "Execution result should be true or false")

	if success {
		t.Logf("Task %s executed successfully", taskID)
	} else {
		t.Logf("Task %s failed during execution", taskID)
	}
}
