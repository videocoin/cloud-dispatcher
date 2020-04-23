package v1

import (
	"context"
	"time"

	grpcmiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpczap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	grpcretry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
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
				grpczap.UnaryClientInterceptor(ctxzap.Extract(ctx)),
				grpcretry.UnaryClientInterceptor(
					grpcretry.WithMax(3),
					grpcretry.WithBackoff(grpcretry.BackoffLinear(500*time.Millisecond)),
				),
			),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 10,
			PermitWithoutStream: true,
		}),
	}
}

func NewDefaultClientValidatorDialOption(ctx context.Context) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			grpcmiddleware.ChainUnaryClient(
				grpctracing.UnaryClientInterceptor(grpctracing.WithTracer(opentracing.GlobalTracer())),
				grpcprometheus.UnaryClientInterceptor,
				grpczap.UnaryClientInterceptor(ctxzap.Extract(ctx)),
			),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 10,
			PermitWithoutStream: true,
		}),
	}
}

func NewDefaultClientDialOptionWithoutRetry(ctx context.Context) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(
			grpcmiddleware.ChainUnaryClient(
				grpctracing.UnaryClientInterceptor(grpctracing.WithTracer(opentracing.GlobalTracer())),
				grpcprometheus.UnaryClientInterceptor,
				grpczap.UnaryClientInterceptor(ctxzap.Extract(ctx)),
			),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                time.Second * 10,
			Timeout:             time.Second * 10,
			PermitWithoutStream: true,
		}),
	}
}
