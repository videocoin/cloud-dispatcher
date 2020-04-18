package logger

import (
	"os"
	"strings"
	"time"

	"github.com/TheZeroSlave/zapsentry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewZapLogger(serviceName string, serviceVersion string) *zap.Logger {
	zapLogger, _ := zap.NewProduction()

	sentryDSN = os.Getenv("SENTRY_DSN")

	loglevel = strings.ToLower(os.Getenv("LOGLEVEL"))
	if loglevel == "" {
		loglevel = "info"
	}

	if loglevel == "debug" {
		zapLogger, _ = zap.NewDevelopment()
	}

	zapLogger = zapLogger.With(
		zap.String("service", serviceName),
		zap.String("version", serviceVersion),
	)

	if sentryDSN != "" {
		sentryCfg := zapsentry.Configuration{
			Level:        zapcore.ErrorLevel,
			FlushTimeout: 5 * time.Second,
		}

		sentryCore, err := zapsentry.NewCore(sentryCfg, zapsentry.NewSentryClientFromDSN(sentryDSN))
		if err != nil {
			zapLogger.Fatal("failed to init zap", zap.Error(err))
		}

		zapLogger = zapsentry.AttachCoreToLogger(sentryCore, zapLogger)
	}

	return zapLogger
}
