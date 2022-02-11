package rpc

import (
	"context"
	"net"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpclogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpcctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpctracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	clientv1 "github.com/videocoin/cloud-api/client/v1"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-dispatcher/eventbus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

type ServerOpts struct {
	Addr               string
	SyncerURL          string
	RPCNodeURL         string
	StakingManagerAddr string
	Mode               *Mode
	DM                 *datastore.DataManager
	EB                 *eventbus.EventBus
	SC                 *clientv1.ServiceClient
}

type Mode struct {
	OnlyInternal   bool
	MinimalVersion string
}

type Server struct {
	logger             *logrus.Entry
	mode               *Mode
	addr               string
	syncerURL          string
	rpcNodeURL         string
	stakingManagerAddr string
	grpc               *grpc.Server
	listen             net.Listener
	sc                 *clientv1.ServiceClient
	dm                 *datastore.DataManager
	eb                 *eventbus.EventBus
}

func NewServer(ctx context.Context, opts *ServerOpts) (*Server, error) {
	grpcOpts := []grpc.ServerOption{
		grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer(
			grpcctxtags.UnaryServerInterceptor(),
			grpctracing.UnaryServerInterceptor(
				grpctracing.WithTracer(opentracing.GlobalTracer()),
				grpctracing.WithFilterFunc(tracingFilter),
			),
			grpcprometheus.UnaryServerInterceptor,
			grpclogrus.UnaryServerInterceptor(grpclogrus.Extract(ctx), grpclogrus.WithDecider(logrusFilter)),
			grpcauth.UnaryServerInterceptor(nopAuth),
		)),
	}

	grpcServer := grpc.NewServer(grpcOpts...)
	listen, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, err
	}

	rpcServer := &Server{
		logger:             grpclogrus.Extract(ctx).WithField("system", "rpc"),
		addr:               opts.Addr,
		syncerURL:          opts.SyncerURL,
		rpcNodeURL:         opts.RPCNodeURL,
		stakingManagerAddr: opts.StakingManagerAddr,
		mode:               opts.Mode,
		sc:                 opts.SC,
		dm:                 opts.DM,
		eb:                 opts.EB,
		grpc:               grpcServer,
		listen:             listen,
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
