package rpc

import "errors"

var (
	ErrWorkerStateIsBonding   = errors.New("worker state is BONDING")
	ErrWorkerStateIsUnbonded  = errors.New("worker state is UNBONDED")
	ErrWorkerStateIsUnbonding = errors.New("worker state is UNBONDING")
	ErrWrongWorkerState       = errors.New("wrong worker state")
	ErrWorkerAddressIsEmpty   = errors.New("worker address is empty")
)
