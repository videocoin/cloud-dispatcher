package rpc

import (
	"context"
	"net"

	protoempty "github.com/gogo/protobuf/types"
	"github.com/sirupsen/logrus"
	accountsv1 "github.com/videocoin/cloud-api/accounts/v1"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	"github.com/videocoin/cloud-api/rpc"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
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
	v         *requestValidator
	dm        *datastore.DataManager
}

func NewRpcServer(opts *RpcServerOpts) (*RpcServer, error) {
	grpcOpts := grpcutil.DefaultServerOpts(opts.Logger)
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
