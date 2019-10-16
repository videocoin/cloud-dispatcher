package grpcutil

import (
	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpclogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpctags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpctracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcvalidator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func DefaultServerOpts(logger *logrus.Entry) []grpc.ServerOption {
	// grpclogrus.ReplaceGrpcLogger(logger)

	tracerOpts := grpctracing.WithTracer(opentracing.GlobalTracer())

	return []grpc.ServerOption{
		grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer(
			grpclogrus.UnaryServerInterceptor(logger),
			grpctags.UnaryServerInterceptor(),
			grpctracing.UnaryServerInterceptor(tracerOpts),
			grpcprometheus.UnaryServerInterceptor,
			grpcvalidator.UnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpcmiddleware.ChainStreamServer(
			grpclogrus.StreamServerInterceptor(logger),
			grpctags.StreamServerInterceptor(),
			grpctracing.StreamServerInterceptor(tracerOpts),
			grpcprometheus.StreamServerInterceptor,
			grpcvalidator.StreamServerInterceptor(),
		)),
	}
}
