package logger

import (
	"os"
	"time"

	logrussentry "github.com/evalphobia/logrus_sentry"
	logrusloki "github.com/schoentoon/logrus-loki"
	"github.com/sirupsen/logrus"
)

func NewLogrusLogger(serviceName string, serviceVersion string, lokiURL *string) *logrus.Entry {
	l := logrus.New()

	loglevel = os.Getenv("LOGLEVEL")
	if loglevel == "" {
		loglevel = logrus.InfoLevel.String()
	}
	level, err := logrus.ParseLevel(loglevel)
	if err != nil {
		level = logrus.InfoLevel
	}

	l.SetLevel(level)
	l.SetFormatter(&logrus.JSONFormatter{TimestampFormat: time.RFC3339Nano})

	sentryDSN = os.Getenv("SENTRY_DSN")
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
			l.Warning(err)
		} else {
			l.AddHook(sentryHook)
		}
	}

	if lokiURL != nil {
		lokiHook, err := logrusloki.NewLoki(*lokiURL, 1024, 2)
		lokiHook.AddData("app", serviceName)
		lokiHook.AddData("version", serviceVersion)
		if err != nil {
			l.Warning(err)
		} else {
			l.AddHook(lokiHook)
		}
	}

	logger := logrus.
		NewEntry(l).
		WithFields(logrus.Fields{
			"service": serviceName,
			"version": serviceVersion,
		})

	return logger
}
