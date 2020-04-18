package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/kelseyhightower/envconfig"
	"github.com/videocoin/cloud-dispatcher/service"
	"github.com/videocoin/cloud-pkg/logger"
	"github.com/videocoin/cloud-pkg/tracer"
	"go.uber.org/zap"
)

var (
	ServiceName string = "dispatcher"
	Version     string = "dev"
)

func main() {
	log := logger.NewZapLogger(ServiceName, Version)

	closer, err := tracer.NewTracer(ServiceName)
	if err != nil {
		log.Warn("failed to create tracer", zap.Error(err))
	} else {
		defer closer.Close()
	}

	cfg := &service.Config{
		Name:    ServiceName,
		Version: Version,
	}

	err = envconfig.Process(ServiceName, cfg)
	if err != nil {
		log.Fatal("failed to process env config", zap.Error(err))
	}

	ctx := ctxzap.ToContext(context.Background(), log)
	svc, err := service.NewService(ctx, cfg)
	if err != nil {
		log.Fatal("failed to create service", zap.Error(err))
	}

	signals := make(chan os.Signal, 1)
	exit := make(chan bool, 1)
	errCh := make(chan error, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals

		log.Info("recieved signal", zap.String("signal", sig.String()))
		exit <- true
	}()

	log.Info("starting")
	go svc.Start(errCh)

	select {
	case <-exit:
		break
	case err := <-errCh:
		if err != nil {
			log.Error("failed to start service", zap.Error(err))
		}
		break
	}

	log.Info("stopping")
	err = svc.Stop()
	if err != nil {
		log.Error("failed to stop service", zap.Error(err))
		return
	}

	log.Info("stopped")
}
