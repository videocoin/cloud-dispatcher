package grpcutil

import (
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpclogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	grpctracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func DefaultClientDialOpts(logger *logrus.Entry) []grpc.DialOption {
	tracerOpts := grpctracing.WithTracer(opentracing.GlobalTracer())

	return []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			grpc.UnaryClientInterceptor(grpcmiddleware.ChainUnaryClient(
				grpclogrus.UnaryClientInterceptor(logger),
				grpctracing.UnaryClientInterceptor(tracerOpts),
				grpcprometheus.UnaryClientInterceptor,
			)),
		),
		grpc.WithStreamInterceptor(
			grpc.StreamClientInterceptor(grpcmiddleware.ChainStreamClient(
				grpclogrus.StreamClientInterceptor(logger),
				grpctracing.StreamClientInterceptor(tracerOpts),
				grpcprometheus.StreamClientInterceptor,
			)),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 10,
			PermitWithoutStream: true,
		}),
	}
}

func ClientDialOptsWithRetry(logger *logrus.Entry) []grpc.DialOption {
	tracerOpts := grpctracing.WithTracer(opentracing.GlobalTracer())
	retryOpts := []grpcretry.CallOption{
		grpcretry.WithMax(3),
		grpcretry.WithPerRetryTimeout(1 * time.Second),
		grpcretry.WithBackoff(grpcretry.BackoffLinear(300 * time.Millisecond)),
	}

	return []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			grpc.UnaryClientInterceptor(grpcmiddleware.ChainUnaryClient(
				grpclogrus.UnaryClientInterceptor(logger),
				grpctracing.UnaryClientInterceptor(tracerOpts),
				grpcprometheus.UnaryClientInterceptor,
				grpcretry.UnaryClientInterceptor(retryOpts...),
			)),
		),
		grpc.WithStreamInterceptor(
			grpc.StreamClientInterceptor(grpcmiddleware.ChainStreamClient(
				grpclogrus.StreamClientInterceptor(logger),
				grpctracing.StreamClientInterceptor(tracerOpts),
				grpcprometheus.StreamClientInterceptor,
				grpcretry.StreamClientInterceptor(retryOpts...),
			)),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 10,
			PermitWithoutStream: true,
		}),
	}
}
