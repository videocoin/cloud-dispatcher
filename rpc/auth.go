package rpc

import (
	"context"
	"strings"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	"github.com/videocoin/cloud-pkg/grpcutil"
)

func nopAuth(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (s *Server) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	noAuthSuffixMethods := []string{"GetInternalConfig", "Health", "AddInputChunk"}
	for _, suffix := range noAuthSuffixMethods {
		if strings.HasSuffix(fullMethodName, suffix) {
			return ctx, nil
		}
	}

	clientID, err := grpcauth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}

	miner, err := s.sc.Miners.GetByID(ctx, &minersv1.MinerRequest{Id: clientID})
	if err != nil {
		if grpcutil.IsNotFoundError(err) {
			return nil, rpc.ErrRpcUnauthenticated
		}

		ctxlogrus.Extract(ctx).WithError(err).Error("failed to get miner")

		return nil, rpc.ErrRpcUnauthenticated
	}

	if miner == nil {
		return nil, rpc.ErrRpcUnauthenticated
	}

	return NewContextWithMiner(ctx, miner), nil
}
