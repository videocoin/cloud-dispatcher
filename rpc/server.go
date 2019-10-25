package rpc

import (
	"context"
	"net"

	protoempty "github.com/gogo/protobuf/types"
	"github.com/sirupsen/logrus"
	accountsv1 "github.com/videocoin/cloud-api/accounts/v1"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	syncerv1 "github.com/videocoin/cloud-api/syncer/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-pkg/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type RpcServerOpts struct {
	Logger    *logrus.Entry
	Addr      string
	DM        *datastore.DataManager
	Accounts  accountsv1.AccountServiceClient
	Emitter   emitterv1.EmitterServiceClient
	Streams   streamsv1.StreamsServiceClient
	Validator validatorv1.ValidatorServiceClient
	Syncer    syncerv1.SyncerServiceClient
	Miners    minersv1.MinersServiceClient
}

type RpcServer struct {
	logger    *logrus.Entry
	addr      string
	grpc      *grpc.Server
	listen    net.Listener
	accounts  accountsv1.AccountServiceClient
	emitter   emitterv1.EmitterServiceClient
	streams   streamsv1.StreamsServiceClient
	validator validatorv1.ValidatorServiceClient
	syncer    syncerv1.SyncerServiceClient
	miners    minersv1.MinersServiceClient
	v         *requestValidator
	dm        *datastore.DataManager
}

func NewRpcServer(opts *RpcServerOpts) (*RpcServer, error) {
	grpcOpts := grpcutil.DefaultServerOpts(opts.Logger)
	grpcOpts = append(grpcOpts, grpc.MaxRecvMsgSize(1024*1024*1024))
	grpcOpts = append(grpcOpts, grpc.MaxSendMsgSize(1024*1024*1024))
	grpcServer := grpc.NewServer(grpcOpts...)

	listen, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, err
	}

	rpcServer := &RpcServer{
		addr:      opts.Addr,
		grpc:      grpcServer,
		listen:    listen,
		accounts:  opts.Accounts,
		emitter:   opts.Emitter,
		streams:   opts.Streams,
		validator: opts.Validator,
		syncer:    opts.Syncer,
		miners:    opts.Miners,
		logger:    opts.Logger.WithField("system", "rpc"),
		v:         newRequestValidator(),
		dm:        opts.DM,
	}

	v1.RegisterDispatcherServiceServer(grpcServer, rpcServer)
	reflection.Register(grpcServer)

	return rpcServer, nil
}

func (s *RpcServer) Start() error {
	s.logger.Infof("starting rpc server on %s", s.addr)
	return s.grpc.Serve(s.listen)
}

func (s *RpcServer) Health(ctx context.Context, req *protoempty.Empty) (*rpc.HealthStatus, error) {
	return &rpc.HealthStatus{Status: "OK"}, nil
}
