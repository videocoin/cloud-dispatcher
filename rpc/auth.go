package rpc

import (
	"context"
	"errors"

	"github.com/opentracing/opentracing-go"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
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

func AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	// noAuthMethods := []string{}
	return ctx, nil
}
