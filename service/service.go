package service

import (
	accountsv1 "github.com/videocoin/cloud-api/accounts/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	profilesv1 "github.com/videocoin/cloud-api/profiles/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-dispatcher/eventbus"
	"github.com/videocoin/cloud-dispatcher/rpc"
	"github.com/videocoin/cloud-pkg/grpcutil"
	"google.golang.org/grpc"
)

type Service struct {
	cfg *Config
	rpc *rpc.RpcServer
	eb  *eventbus.EventBus
}

func NewService(cfg *Config) (*Service, error) {
	alogger := cfg.Logger.WithField("system", "accountcli")
	aGrpcDialOpts := grpcutil.ClientDialOptsWithRetry(alogger)
	accountsConn, err := grpc.Dial(cfg.AccountsRPCAddr, aGrpcDialOpts...)
	if err != nil {
		return nil, err
	}
	accounts := accountsv1.NewAccountServiceClient(accountsConn)

	elogger := cfg.Logger.WithField("system", "emittercli")
	eGrpcDialOpts := grpcutil.ClientDialOptsWithRetry(elogger)
	emitterConn, err := grpc.Dial(cfg.EmitterRPCAddr, eGrpcDialOpts...)
	if err != nil {
		return nil, err
	}
	emitter := emitterv1.NewEmitterServiceClient(emitterConn)

	slogger := cfg.Logger.WithField("system", "pstreamscli")
	sGrpcDialOpts := grpcutil.ClientDialOptsWithRetry(slogger)
	streamsConn, err := grpc.Dial(cfg.StreamsRPCAddr, sGrpcDialOpts...)
	if err != nil {
		return nil, err
	}
	streams := streamsv1.NewStreamsServiceClient(streamsConn)

	plogger := cfg.Logger.WithField("system", "profilescli")
	pGrpcDialOpts := grpcutil.ClientDialOptsWithRetry(plogger)
	profilesConn, err := grpc.Dial(cfg.ProfilesRPCAddr, pGrpcDialOpts...)
	if err != nil {
		return nil, err
	}
	profiles := profilesv1.NewProfilesServiceClient(profilesConn)

	ds, err := datastore.NewDatastore(cfg.DBURI)
	if err != nil {
		return nil, err
	}

	dm, err := datastore.NewDataManager(
		ds,
		streams,
		profiles,
		cfg.Logger.WithField("system", "datamanager"),
	)
	if err != nil {
		return nil, err
	}

	rpcConfig := &rpc.RpcServerOpts{
		Addr:     cfg.RPCAddr,
		Accounts: accounts,
		Emitter:  emitter,
		Streams:  streams,
		Logger:   cfg.Logger,
		DM:       dm,
	}

	rpc, err := rpc.NewRpcServer(rpcConfig)
	if err != nil {
		return nil, err
	}

	ebConfig := &eventbus.Config{
		URI:    cfg.MQURI,
		Name:   cfg.Name,
		Logger: cfg.Logger.WithField("system", "eventbus"),
		DM:     dm,
	}
	eb, err := eventbus.New(ebConfig)
	if err != nil {
		return nil, err
	}

	svc := &Service{
		cfg: cfg,
		rpc: rpc,
		eb:  eb,
	}

	return svc, nil
}

func (s *Service) Start() error {
	go s.rpc.Start()
	go s.eb.Start()
	return nil
}

func (s *Service) Stop() error {
	s.eb.Stop()
	return nil
}
