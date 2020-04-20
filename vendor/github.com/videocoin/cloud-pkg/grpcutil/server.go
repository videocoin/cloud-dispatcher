package grpcutil

import (
	"context"

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

	tracerOpts := []grpctracing.Option{
		grpctracing.WithTracer(opentracing.GlobalTracer()),
		grpctracing.WithFilterFunc(func(ctx context.Context, fullMethodName string) bool {
			if fullMethodName == "/grpc.health.v1.Health/Check" {
				return false
			}
			return true
		}),
	}
	logrusOpts := []grpclogrus.Option{
		grpclogrus.WithDecider(func(methodFullName string, err error) bool {
			if methodFullName == "/grpc.health.v1.Health/Check" {
				return false
			}
			return true
		}),
	}

	return []grpc.ServerOption{
		grpc.UnaryInterceptor(grpcmiddleware.ChainUnaryServer(
			grpclogrus.UnaryServerInterceptor(logger, logrusOpts...),
			grpctags.UnaryServerInterceptor(),
			grpctracing.UnaryServerInterceptor(tracerOpts...),
			grpcprometheus.UnaryServerInterceptor,
			grpcvalidator.UnaryServerInterceptor(),
		)),
		grpc.StreamInterceptor(grpcmiddleware.ChainStreamServer(
			grpclogrus.StreamServerInterceptor(logger),
			grpctags.StreamServerInterceptor(),
			grpctracing.StreamServerInterceptor(tracerOpts...),
			grpcprometheus.StreamServerInterceptor,
			grpcvalidator.StreamServerInterceptor(),
		)),
	}
}
