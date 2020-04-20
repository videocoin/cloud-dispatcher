package rpc

import (
	"context"
	"errors"
	"strings"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/opentracing/opentracing-go"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	"github.com/videocoin/cloud-pkg/grpcutil"
	"go.uber.org/zap"
)

var (
	ErrClientIDIsEmpty  = errors.New("client id is empty")
	ErrClientIDNotFound = errors.New("client id not found")
)

func (s *Server) authenticate(ctx context.Context, clientID string) (*minersv1.MinerResponse, error) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "rpc.authenticate")
	defer span.Finish()

	if clientID == "" {
		return nil, ErrClientIDIsEmpty
	}

	miner, err := s.sc.Miners.GetByID(spanCtx, &minersv1.MinerRequest{Id: clientID})
	if err != nil {
		return nil, err
	}

	if miner == nil {
		return nil, ErrClientIDNotFound
	}

	return miner, nil
}

func (s *Server) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	noAuthSuffixMethods := []string{"GetInternalConfig"}
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

		ctxzap.Extract(ctx).Error("failed to get miner", zap.Error(err))

		return nil, rpc.ErrRpcUnauthenticated
	}

	if miner == nil {
		return nil, rpc.ErrRpcUnauthenticated
	}

	return NewContextWithMiner(ctx, miner), nil
}
