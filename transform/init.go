package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"golang.org/x/exp/slog"
	"os"
)

var (
	logger      *slog.Logger
	logLevel    = getEnv("LOG_LEVEL", "INFO")
)

func init() {
	// loads values from .env into the system
	if err := godotenv.Load(); err != nil {
		slog.Debug("No .env file found")
	}
	logger = initLog()
}

func initLog() *slog.Logger {
	switch logLevel {
	case "DEBUG":
		opts := &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}
		logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
		logger.Info(fmt.Sprintf("Loglevel set to: %s", logLevel))
		slog.SetDefault(logger)
		return logger
	default:
		opts := &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}
		logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
		logger.Info(fmt.Sprintf("Loglevel set to: %s", logLevel))
		slog.SetDefault(logger)
		return logger
	}
}
