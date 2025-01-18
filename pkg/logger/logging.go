package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

const (
	LevelTrace   = slog.Level(-8)
	LevelDebug   = slog.LevelDebug
	LevelInfo    = slog.LevelInfo
	LevelNotice  = slog.Level(2)
	LevelWarning = slog.LevelWarn
	LevelError   = slog.LevelError
	LevelFatal   = slog.Level(12)
)

type LoggerConfig struct {
	LogLevel       string // Log level (e.g., INFO, DEBUG, ERROR)
	CommandHandler string // Output format (e.g., json or text)
	AddSource      bool   // Include source file and line number in logs
}

type Logger struct {
	*slog.Logger
}

// NewLogger initializes a new logger based on the provided configuration.
func NewLogger(logConfig LoggerConfig) (*Logger, error) {
	level, err := parseLogLevel(logConfig.LogLevel)
	if err != nil {
		return nil, err
	}

	handlerOptions := &slog.HandlerOptions{
		Level:       level, // Set the minimum log level
		AddSource:   logConfig.AddSource,
		ReplaceAttr: replaceAttr, // Custom attribute formatting
	}

	var handler slog.Handler
	switch strings.ToLower(logConfig.CommandHandler) {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, handlerOptions)
	default:
		handler = slog.NewTextHandler(os.Stdout, handlerOptions)
	}

	logger := slog.New(handler)
	return &Logger{Logger: logger}, nil
}

// Fatal logs a fatal message and exits the application.
func (log *Logger) Fatal(msg string, args ...interface{}) {
	log.Logger.Log(context.Background(), LevelFatal, msg, args...)
	os.Exit(1)
}

// FatalCtx logs a fatal message with context and exits the application.
func (log *Logger) FatalCtx(ctx context.Context, msg string, args ...interface{}) {
	log.Logger.Log(ctx, LevelFatal, msg, args...)
	os.Exit(1)
}

// replaceAttr formats attributes like log level for a custom display.
func replaceAttr(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.LevelKey {
		level := a.Value.Any().(slog.Level)
		a.Value = slog.StringValue(levelToString(level))
	}
	return a
}

// parseLogLevel converts a string log level to the corresponding slog.Level.
func parseLogLevel(level string) (slog.Level, error) {
	switch strings.ToUpper(level) {
	case "TRACE":
		return LevelTrace, nil
	case "DEBUG":
		return LevelDebug, nil
	case "INFO":
		return LevelInfo, nil
	case "NOTICE":
		return LevelNotice, nil
	case "WARNING":
		return LevelWarning, nil
	case "ERROR":
		return LevelError, nil
	case "FATAL":
		return LevelFatal, nil
	default:
		return LevelInfo, nil // Default log level
	}
}

// levelToString converts slog.Level to a human-readable string.
func levelToString(level slog.Level) string {
	switch {
	case level < LevelDebug:
		return "TRACE"
	case level < LevelInfo:
		return "DEBUG"
	case level < LevelNotice:
		return "INFO"
	case level < LevelWarning:
		return "NOTICE"
	case level < LevelError:
		return "WARNING"
	case level < LevelFatal:
		return "ERROR"
	default:
		return "FATAL"
	}
}
