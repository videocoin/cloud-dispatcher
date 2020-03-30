package rpc

import (
	"net"

	"github.com/sirupsen/logrus"
	accountsv1 "github.com/videocoin/cloud-api/accounts/v1"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-dispatcher/eventbus"
	"github.com/videocoin/cloud-pkg/consul"
	"github.com/videocoin/cloud-pkg/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type RpcServerOpts struct {
	Logger     *logrus.Entry
	Addr       string
	DM         *datastore.DataManager
	EB         *eventbus.EventBus
	Accounts   accountsv1.AccountServiceClient
	Emitter    emitterv1.EmitterServiceClient
	Streams    streamsv1.StreamsServiceClient
	Validator  validatorv1.ValidatorServiceClient
	Miners     minersv1.MinersServiceClient
	Consul     *consul.Client
	SyncerURL  string
	RPCNodeURL string
}

type RpcServer struct {
	logger     *logrus.Entry
	addr       string
	grpc       *grpc.Server
	listen     net.Listener
	accounts   accountsv1.AccountServiceClient
	emitter    emitterv1.EmitterServiceClient
	streams    streamsv1.StreamsServiceClient
	validator  validatorv1.ValidatorServiceClient
	miners     minersv1.MinersServiceClient
	v          *requestValidator
	dm         *datastore.DataManager
	eb         *eventbus.EventBus
	consul     *consul.Client
	syncerURL  string
	rpcNodeURL string
}

func NewRpcServer(opts *RpcServerOpts) (*RpcServer, error) {
	grpcOpts := grpcutil.DefaultServerOpts(opts.Logger)
	grpcOpts = append(grpcOpts, grpc.MaxRecvMsgSize(1024*1024*1024))
	grpcOpts = append(grpcOpts, grpc.MaxSendMsgSize(1024*1024*1024))
	grpcServer := grpc.NewServer(grpcOpts...)
	healthService := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthService)
	listen, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, err
	}

	rpcServer := &RpcServer{
		addr:       opts.Addr,
		grpc:       grpcServer,
		listen:     listen,
		accounts:   opts.Accounts,
		emitter:    opts.Emitter,
		streams:    opts.Streams,
		validator:  opts.Validator,
		miners:     opts.Miners,
		logger:     opts.Logger.WithField("system", "rpc"),
		v:          newRequestValidator(),
		dm:         opts.DM,
		eb:         opts.EB,
		consul:     opts.Consul,
		syncerURL:  opts.SyncerURL,
		rpcNodeURL: opts.RPCNodeURL,
	}

	v1.RegisterDispatcherServiceServer(grpcServer, rpcServer)
	reflection.Register(grpcServer)

	return rpcServer, nil
}

func (s *RpcServer) Start() error {
	s.logger.Infof("starting rpc server on %s", s.addr)
	return s.grpc.Serve(s.listen)
}
