package config

import (
	"flag"
	"github.com/mdshahjahanmiah/explore-go/logging"
)

type Config struct {
	ExecutionMode          string
	SimulatedExecutionTime int
	RedisAddress           string
	LoggerConfig           logging.LoggerConfig
}

func Load() (Config, error) {
	fs := flag.NewFlagSet("", flag.ExitOnError)

	executionMode := fs.String("execution.mode", "sequential", "execution mode e.g sequential, otherwise default will be concurrent")
	simulatedExecutionTime := fs.Int("simulated.execution.time", 3, "simulated execution time for task")
	redisAddress := fs.String("redis.address", "localhost:6379", "redis address")

	loggerConfig := logging.LoggerConfig{}
	fs.StringVar(&loggerConfig.CommandHandler, "logger.handler.type", "json", "handler type e.g json, otherwise default will be text type")
	fs.StringVar(&loggerConfig.LogLevel, "logger.log.level", "info", "log level wise logging with fatal log")

	config := Config{
		ExecutionMode:          *executionMode,
		SimulatedExecutionTime: *simulatedExecutionTime,
		RedisAddress:           *redisAddress,
		LoggerConfig:           loggerConfig,
	}

	return config, nil
}
