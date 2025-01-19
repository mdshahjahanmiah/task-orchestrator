package config

import (
	"flag"
	"github.com/mdshahjahanmiah/task-orchestrator/pkg/logger"
)

type Config struct {
	ExecutionMode          string
	SimulatedExecutionTime int
	WorkerCount            int
	RedisAddress           string
	LoggerConfig           logging.LoggerConfig
}

func Load() (Config, error) {
	fs := flag.NewFlagSet("", flag.ExitOnError)

	executionMode := fs.String("execution.mode", "sequential", "execution mode e.g., sequential, otherwise default will be concurrent")
	simulatedExecutionTime := fs.Int("simulated.execution.time", 5, "simulated execution time for task")
	redisAddress := fs.String("redis.address", "localhost:6379", "Redis address")
	workerCount := fs.Int("worker.count", 5, "number of workers")

	loggerConfig := logging.LoggerConfig{}
	fs.StringVar(&loggerConfig.CommandHandler, "logger.handler.type", "json", "handler type e.g., json, otherwise default will be text type")
	fs.StringVar(&loggerConfig.LogLevel, "logger.log.level", "info", "log level (INFO, DEBUG, etc.)")

	// Parse the command-line flags
	err := fs.Parse(flag.Args())
	if err != nil {
		return Config{}, err
	}

	config := Config{
		ExecutionMode:          *executionMode,
		SimulatedExecutionTime: *simulatedExecutionTime,
		WorkerCount:            *workerCount,
		RedisAddress:           *redisAddress,
		LoggerConfig:           loggerConfig,
	}

	return config, nil
}
