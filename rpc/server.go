package rpc

import (
	"context"
	"net"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	grpcctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpctracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/opentracing/opentracing-go"
	clientv1 "github.com/videocoin/cloud-api/client/v1"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-dispatcher/eventbus"
	"github.com/videocoin/cloud-pkg/consul"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type ServerOpts struct {
	Addr       string
	DM         *datastore.DataManager
	EB         *eventbus.EventBus
	SC         *clientv1.ServiceClient
	Consul     *consul.Client
	SyncerURL  string
	RPCNodeURL string
}

type Server struct {
	logger     *zap.Logger
	addr       string
	grpc       *grpc.Server
	listen     net.Listener
	sc         *clientv1.ServiceClient
	dm         *datastore.DataManager
	eb         *eventbus.EventBus
	consul     *consul.Client
	syncerURL  string
	rpcNodeURL string
}

func NewServer(ctx context.Context, opts *ServerOpts) (*Server, error) {
	// grpclogrus.ReplaceGrpcLogger(logger)
	// grpczap.ReplaceGrpcLoggerV2(zapLogger)

	tracerOpts := []grpctracing.Option{
		grpctracing.WithTracer(opentracing.GlobalTracer()),
		grpctracing.WithFilterFunc(func(ctx context.Context, fullMethodName string) bool {
			return fullMethodName != "/grpc.health.v1.Health/Check"
		}),
	}

	zapOpts := []grpczap.Option{
		grpczap.WithDecider(
			func(methodFullName string, err error) bool {
				return methodFullName != "/grpc.health.v1.Health/Check"
			},
		),
	}

	grpcOpts := []grpc.ServerOption{
		grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer(
			grpcctxtags.UnaryServerInterceptor(),
			grpctracing.UnaryServerInterceptor(tracerOpts...),
			grpcprometheus.UnaryServerInterceptor,
			grpczap.UnaryServerInterceptor(ctxzap.Extract(ctx), zapOpts...),
			grpcauth.UnaryServerInterceptor(nil),
		)),
	}

	grpcServer := grpc.NewServer(grpcOpts...)
	listen, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, err
	}

	rpcServer := &Server{
		logger:     ctxzap.Extract(ctx).With(zap.String("system", "rpc")),
		addr:       opts.Addr,
		sc:         opts.SC,
		dm:         opts.DM,
		eb:         opts.EB,
		consul:     opts.Consul,
		syncerURL:  opts.SyncerURL,
		rpcNodeURL: opts.RPCNodeURL,
		grpc:       grpcServer,
		listen:     listen,
	}

	healthSrv := health.NewServer()
	healthv1.RegisterHealthServer(grpcServer, healthSrv)

	v1.RegisterDispatcherServiceServer(grpcServer, rpcServer)
	reflection.Register(grpcServer)

	return rpcServer, nil
}

func (s *Server) Start() error {
	return s.grpc.Serve(s.listen)
}
