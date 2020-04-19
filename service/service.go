package service

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	clientv1 "github.com/videocoin/cloud-api/client/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-dispatcher/eventbus"
	"github.com/videocoin/cloud-dispatcher/metrics"
	"github.com/videocoin/cloud-dispatcher/rpc"
	"github.com/videocoin/cloud-pkg/consul"
	"go.uber.org/zap"
)

type Service struct {
	cfg    *Config
	logger *zap.Logger
	rpc    *rpc.Server
	eb     *eventbus.EventBus
	mc     *metrics.Collector
	ms     *metrics.HTTPServer
}

func NewService(ctx context.Context, cfg *Config) (*Service, error) {
	sc, err := clientv1.NewServiceClientFromEnvconfig(ctx, cfg)
	if err != nil {
		return nil, err
	}

	consulCli, err := consul.NewClient(cfg.Env, cfg.ConsulAddr)
	if err != nil {
		return nil, err
	}

	ds, err := datastore.NewDatastore(cfg.DBURI)
	if err != nil {
		return nil, err
	}

	dm, err := datastore.NewDataManager(ctx, ds, sc)
	if err != nil {
		return nil, err
	}

	eb, err := eventbus.NewEventBus(ctx, cfg.MQURI, cfg.Name, dm, sc)
	if err != nil {
		return nil, err
	}

	rpcConfig := &rpc.ServerOpts{
		Addr:       cfg.RPCAddr,
		RPCNodeURL: cfg.RPCNodeURL,
		SyncerURL:  cfg.SyncerURL,
		Consul:     consulCli,
		SC:         sc,
		DM:         dm,
		EB:         eb,
	}

	rpc, err := rpc.NewServer(ctx, rpcConfig)
	if err != nil {
		return nil, err
	}

	mc := metrics.NewCollector(cfg.Name, dm)

	ms, err := metrics.NewHTTPServer(cfg.MetricsAddr)
	if err != nil {
		return nil, err
	}

	svc := &Service{
		cfg:    cfg,
		logger: ctxzap.Extract(ctx),
		rpc:    rpc,
		eb:     eb,
		mc:     mc,
		ms:     ms,
	}

	return svc, nil
}

func (s *Service) Start(errCh chan error) {
	go func() {
		s.logger.Info("starting rpc server", zap.String("addr", s.cfg.RPCAddr))
		errCh <- s.rpc.Start()
	}()

	go func() {
		s.logger.Info("starting eventbus")
		errCh <- s.eb.Start()
	}()

	go func() {
		s.logger.Info("starting metrics server", zap.String("addr", s.cfg.MetricsAddr))
		errCh <- s.ms.Start()
	}()

	s.mc.Start()
}

func (s *Service) Stop() error {
	err := s.eb.Stop()
	if err != nil {
		return err
	}

	err = s.ms.Stop()
	if err != nil {
		return err
	}

	s.mc.Stop()

	return nil
}
