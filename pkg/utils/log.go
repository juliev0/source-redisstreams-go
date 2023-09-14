package utils

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger returns a new zap.SugaredLogger
func NewLogger() *zap.SugaredLogger {
	var config zap.Config
	debugMode, ok := os.LookupEnv("NUMAFLOW_DEBUG")
	if ok && debugMode == "true" {
		config = zap.NewDevelopmentConfig()
	} else {
		config = zap.NewProductionConfig()
	}
	// Config customization goes here if any
	config.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	config.OutputPaths = []string{"stdout"}
	logger, err := config.Build()
	if err != nil {
		panic(err)
	}
	return logger.Named("redis-streams-source").Sugar()
}
