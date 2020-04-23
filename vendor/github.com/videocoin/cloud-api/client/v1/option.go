package v1

import (
	"context"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpclogrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpctracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	grpcprometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func NewDefaultClientDialOption(ctx context.Context) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			grpcmiddleware.ChainUnaryClient(
				grpctracing.UnaryClientInterceptor(grpctracing.WithTracer(opentracing.GlobalTracer())),
				grpcprometheus.UnaryClientInterceptor,
				grpclogrus.UnaryClientInterceptor(grpclogrus.Extract(ctx)),
			),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 10,
			PermitWithoutStream: true,
		}),
	}
}
