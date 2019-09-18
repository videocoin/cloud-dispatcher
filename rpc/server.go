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
	streamsv1 "github.com/videocoin/cloud-api/streams/v1"
	"github.com/videocoin/cloud-pkg/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type RpcServerOpts struct {
	Addr     string
	Accounts accountsv1.AccountServiceClient
	Emitter  emitterv1.EmitterServiceClient
	Streams  streamsv1.StreamServiceClient
	Logger   *logrus.Entry
}

type RpcServer struct {
	addr      string
	grpc      *grpc.Server
	listen    net.Listener
	accounts  accountsv1.AccountServiceClient
	emitter   emitterv1.EmitterServiceClient
	streams   streamsv1.StreamServiceClient
	validator *requestValidator
	logger    *logrus.Entry
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
		logger:    opts.Logger.WithField("system", "rpc"),
		validator: newRequestValidator(),
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
