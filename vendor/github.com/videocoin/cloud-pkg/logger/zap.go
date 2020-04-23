package logger

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/TheZeroSlave/zapsentry"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewZapLogger(serviceName string, serviceVersion string) *zap.Logger {
	zapLogger, _ := zap.NewProduction()

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

	sentryDSN = os.Getenv("SENTRY_DSN")
	if sentryDSN != "" {
		sentryZapCfg := zapsentry.Configuration{
			Level:        zapcore.ErrorLevel,
			FlushTimeout: 5 * time.Second,
			Tags: map[string]string{
				"service": serviceName,
				"version": serviceVersion,
			},
		}
		sentryCli, err := sentry.NewClient(sentry.ClientOptions{
			Dsn:              sentryDSN,
			AttachStacktrace: true,
			Release:          fmt.Sprintf("%s@%s", serviceName, serviceVersion),
		})
		if err != nil {
			zapLogger.Fatal("failed to new sentry client", zap.Error(err))
		}

		sentryFactory := zapsentry.NewSentryClientFromClient(sentryCli)
		sentryCore, err := zapsentry.NewCore(sentryZapCfg, sentryFactory)
		if err != nil {
			zapLogger.Fatal("failed to init zap", zap.Error(err))
		}

		zapLogger = zapsentry.AttachCoreToLogger(sentryCore, zapLogger)
	}

	return zapLogger
}
