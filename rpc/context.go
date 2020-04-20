package rpc

import (
	"context"

	minersv1 "github.com/videocoin/cloud-api/miners/v1"
)

type key int

const (
	minerKey key = 1
)

func NewContextWithMiner(ctx context.Context, miner *minersv1.MinerResponse) context.Context {
	return context.WithValue(ctx, minerKey, miner)
}

func MinerFromContext(ctx context.Context) (*minersv1.MinerResponse, bool) {
	miner, ok := ctx.Value(minerKey).(*minersv1.MinerResponse)
	return miner, ok
}
