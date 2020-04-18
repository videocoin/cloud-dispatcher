package service

import (
	"context"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpctracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/opentracing/opentracing-go"
	accountsv1 "github.com/videocoin/cloud-api/accounts/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	profilesv1 "github.com/videocoin/cloud-api/profiles/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-dispatcher/eventbus"
	"github.com/videocoin/cloud-dispatcher/metrics"
	"github.com/videocoin/cloud-dispatcher/rpc"
	"github.com/videocoin/cloud-pkg/consul"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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
	grpcClientOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			grpcmiddleware.ChainUnaryClient(
				grpctracing.UnaryClientInterceptor(grpctracing.WithTracer(opentracing.GlobalTracer())),
				grpcprometheus.UnaryClientInterceptor,
				grpczap.UnaryClientInterceptor(ctxzap.Extract(ctx)),
				grpcretry.UnaryClientInterceptor(
					grpcretry.WithMax(3),
					grpcretry.WithBackoff(grpcretry.BackoffLinear(500*time.Millisecond)),
				),
			),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 10,
			PermitWithoutStream: true,
		}),
	}

	clientConn, err := grpc.Dial(cfg.AccountsRPCAddr, grpcClientOpts...)
	if err != nil {
		return nil, err
	}
	accounts := accountsv1.NewAccountServiceClient(clientConn)

	clientConn, err = grpc.Dial(cfg.EmitterRPCAddr, grpcClientOpts...)
	if err != nil {
		return nil, err
	}
	emitter := emitterv1.NewEmitterServiceClient(clientConn)

	clientConn, err = grpc.Dial(cfg.StreamsRPCAddr, grpcClientOpts...)
	if err != nil {
		return nil, err
	}
	streams := streamsv1.NewStreamsServiceClient(clientConn)

	clientConn, err = grpc.Dial(cfg.ProfilesRPCAddr, grpcClientOpts...)
	if err != nil {
		return nil, err
	}
	profiles := profilesv1.NewProfilesServiceClient(clientConn)

	clientConn, err = grpc.Dial(cfg.ValidatorRPCAddr, grpcClientOpts...)
	if err != nil {
		return nil, err
	}
	validator := validatorv1.NewValidatorServiceClient(clientConn)

	clientConn, err = grpc.Dial(cfg.MinersRPCAddr, grpcClientOpts...)
	if err != nil {
		return nil, err
	}
	miners := minersv1.NewMinersServiceClient(clientConn)

	consulCli, err := consul.NewClient(cfg.Env, cfg.ConsulAddr)
	if err != nil {
		return nil, err
	}

	ds, err := datastore.NewDatastore(cfg.DBURI)
	if err != nil {
		return nil, err
	}

	dm, err := datastore.NewDataManager(ctx, ds, streams, profiles)
	if err != nil {
		return nil, err
	}

	ebConfig := &eventbus.Config{
		URI:     cfg.MQURI,
		Name:    cfg.Name,
		DM:      dm,
		Streams: streams,
		Miners:  miners,
	}
	eb, err := eventbus.New(ctx, ebConfig)
	if err != nil {
		return nil, err
	}

	rpcConfig := &rpc.ServerOpts{
		Addr:       cfg.RPCAddr,
		Accounts:   accounts,
		Emitter:    emitter,
		Streams:    streams,
		Validator:  validator,
		Miners:     miners,
		DM:         dm,
		EB:         eb,
		Consul:     consulCli,
		RPCNodeURL: cfg.RPCNodeURL,
		SyncerURL:  cfg.SyncerURL,
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
