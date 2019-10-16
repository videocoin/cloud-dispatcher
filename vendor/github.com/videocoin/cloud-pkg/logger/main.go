package logger

import (
	"fmt"
	"os"
	"time"

	logrussentry "github.com/evalphobia/logrus_sentry"
	"github.com/sirupsen/logrus"
)

var (
	loglevel  string
	sentryDSN string
)

func Init(serviceName string, serviceVersion string) error {
	sentryDSN = os.Getenv("SENTRY_DSN")

	loglevel = os.Getenv("LOGLEVEL")
	if loglevel == "" {
		loglevel = logrus.InfoLevel.String()
	}

	level, err := logrus.ParseLevel(loglevel)
	if err != nil {
		return fmt.Errorf("not a valid log level: %q", loglevel)
	}

	logrus.SetLevel(level)

	if level == logrus.DebugLevel {
		logrus.SetFormatter(&logrus.TextFormatter{TimestampFormat: time.RFC3339Nano})
	} else {
		logrus.SetFormatter(&logrus.JSONFormatter{TimestampFormat: time.RFC3339Nano})
	}

	if sentryDSN != "" {
		sentryLevels := []logrus.Level{
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
		}
		sentryTags := map[string]string{
			"service": serviceName,
			"version": serviceVersion,
		}
		sentryHook, err := logrussentry.NewAsyncWithTagsSentryHook(
			sentryDSN,
			sentryTags,
			sentryLevels,
		)
		sentryHook.StacktraceConfiguration.Enable = true
		sentryHook.Timeout = 5 * time.Second
		sentryHook.SetRelease(serviceVersion)

		if err != nil {
			logrus.Warning(err)
		} else {
			logrus.AddHook(sentryHook)
		}
	}

	return nil
}
