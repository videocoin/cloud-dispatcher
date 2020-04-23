package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/kelseyhightower/envconfig"
	"github.com/videocoin/cloud-dispatcher/service"
	pkglogger "github.com/videocoin/cloud-pkg/logger"
	"github.com/videocoin/cloud-pkg/tracer"
)

var (
	ServiceName string = "dispatcher"
	Version     string = "dev"
)

func main() {
	logger := pkglogger.NewLogrusLogger(ServiceName, Version)

	closer, err := tracer.NewTracer(ServiceName)
	if err != nil {
		logger.WithError(err).Warn("failed to create tracer")
	} else {
		defer closer.Close()
	}

	cfg := &service.Config{
		Name:    ServiceName,
		Version: Version,
	}

	err = envconfig.Process(ServiceName, cfg)
	if err != nil {
		logger.WithError(err).Fatal("failed to process env config")
	}

	ctx := ctxlogrus.ToContext(context.Background(), logger)
	svc, err := service.NewService(ctx, cfg)
	if err != nil {
		logger.WithError(err).Fatal("failed to create service")
	}

	signals := make(chan os.Signal, 1)
	exit := make(chan bool, 1)
	errCh := make(chan error, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals

		logger.WithField("signal", sig.String()).Info("recieved signal")
		exit <- true
	}()

	logger.Info("starting")
	go svc.Start(errCh)

	select {
	case <-exit:
		break
	case err := <-errCh:
		if err != nil {
			logger.WithError(err).Error("failed to start service")
		}
		break
	}

	logger.Info("stopping")
	err = svc.Stop()
	if err != nil {
		logger.WithError(err).Error("failed to stop service")
		return
	}

	logger.Info("stopped")
}
