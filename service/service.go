package service

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	clientv1 "github.com/videocoin/cloud-api/client/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-dispatcher/eventbus"
	"github.com/videocoin/cloud-dispatcher/metrics"
	"github.com/videocoin/cloud-dispatcher/rpc"
	"github.com/videocoin/cloud-pkg/iam"
)

type Service struct {
	cfg    *Config
	logger *logrus.Entry
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

	iamCli, err := iam.NewClient(cfg.IamEndpoint)
	if err != nil {
		return nil, err
	}

	rpcConfig := &rpc.ServerOpts{
		Addr:            cfg.RPCAddr,
		RPCNodeURL:      cfg.RPCNodeURL,
		SyncerURL:       cfg.SyncerURL,
		DelegatorUserID: cfg.DelegatorUserID,
		DelegatorToken:  cfg.DelegatorToken,
		Mode: &rpc.Mode{
			OnlyInternal:   cfg.ModeOnlyInternal,
			MinimalVersion: cfg.ModeMinimalVersion,
		},
		SC:  sc,
		DM:  dm,
		EB:  eb,
		IAM: iamCli,
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
		logger: ctxlogrus.Extract(ctx),
		rpc:    rpc,
		eb:     eb,
		mc:     mc,
		ms:     ms,
	}

	return svc, nil
}

func (s *Service) Start(errCh chan error) {
	go func() {
		s.logger.WithField("addr", s.cfg.RPCAddr).Info("starting rpc server")
		errCh <- s.rpc.Start()
	}()

	go func() {
		s.logger.Info("starting eventbus")
		errCh <- s.eb.Start()
	}()

	go func() {
		s.logger.WithField("addr", s.cfg.MetricsAddr).Info("starting metrics server")
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
