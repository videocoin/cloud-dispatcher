package rpc

import (
	"context"

	prototypes "github.com/gogo/protobuf/types"
	"github.com/jinzhu/copier"
	"github.com/mailru/dbr"
	"github.com/sirupsen/logrus"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	syncerv1 "github.com/videocoin/cloud-api/syncer/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
)

func (s *RpcServer) GetPendingTask(ctx context.Context, req *v1.TaskPendingRequest) (*v1.Task, error) {
	task, err := s.dm.GetPendingTask(ctx)
	if err != nil {
		logFailedTo(s.logger, "get pending task", err)
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	task.ClientID = dbr.NewNullString(req.ClientID)
	err = s.dm.MarkTaskAsAssigned(ctx, task)
	if err != nil {
		logFailedTo(s.logger, "mark as assigned", err)
		return nil, rpc.ErrRpcInternal
	}

	v1Task := &v1.Task{}
	err = copier.Copy(v1Task, task)
	if err != nil {
		logFailedTo(s.logger, "copy task", err)
		return nil, rpc.ErrRpcInternal
	}

	v1Task.ClientID = task.ClientID.String
	v1Task.StreamContractID = uint64(task.StreamContractID.Int64)
	v1Task.StreamContractAddress = task.StreamContractAddress.String

	return v1Task, nil
}

func (s *RpcServer) GetTask(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	task, err := s.dm.GetTaskByID(ctx, req.ID)
	if err != nil {
		logFailedTo(s.logger, "get task", err)
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	v1Task := &v1.Task{}
	err = copier.Copy(v1Task, task)
	if err != nil {
		logFailedTo(s.logger, "copy task", err)
		return nil, rpc.ErrRpcInternal
	}

	v1Task.ClientID = task.ClientID.String

	return v1Task, nil
}

func (s *RpcServer) MarkTaskAsCompleted(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	task, err := s.dm.GetTaskByID(ctx, req.ID)
	if err != nil {
		logFailedTo(s.logger, "get task", err)
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	err = s.dm.MarkTaskAsCompleted(ctx, task)
	if err != nil {
		logFailedTo(s.logger, "mark task as completed", err)
		return nil, rpc.ErrRpcInternal
	}

	v1Task := &v1.Task{}
	err = copier.Copy(v1Task, task)
	if err != nil {
		logFailedTo(s.logger, "copy task", err)
		return nil, rpc.ErrRpcInternal
	}

	v1Task.ClientID = task.ClientID.String

	return v1Task, nil
}

func (s *RpcServer) MarkTaskAsFailed(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	task, err := s.dm.GetTaskByID(ctx, req.ID)
	if err != nil {
		logFailedTo(s.logger, "get task", err)
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	err = s.dm.MarkTaskAsFailed(ctx, task)
	if err != nil {
		logFailedTo(s.logger, "mark task as failed", err)
		return nil, rpc.ErrRpcInternal
	}

	v1Task := &v1.Task{}
	err = copier.Copy(v1Task, task)
	if err != nil {
		logFailedTo(s.logger, "copy task", err)
		return nil, rpc.ErrRpcInternal
	}

	v1Task.ClientID = task.ClientID.String

	return v1Task, nil
}

func (s *RpcServer) ValidateProof(
	ctx context.Context,
	req *validatorv1.ValidateProofRequest,
) (*prototypes.Empty, error) {
	return s.validator.ValidateProof(ctx, req)
}

func (s *RpcServer) Sync(
	ctx context.Context,
	req *syncerv1.SyncRequest,
) (*prototypes.Empty, error) {
	s.logger.WithFields(logrus.Fields{
		"object_name": req.Path,
	}).Info("syncing")

	return s.syncer.Sync(ctx, req)
}

func (s *RpcServer) Ping(
	ctx context.Context,
	req *minersv1.PingRequest,
) (*minersv1.PingResponse, error) {
	s.logger.WithFields(logrus.Fields{
		"client_id": req.ClientID,
	}).Info("ping")

	go func() {
		_, err := s.miners.Ping(context.Background(), req)
		if err != nil {
			s.logger.Errorf("failed to ping: %s", err)
		}
	}()

	return &minersv1.PingResponse{}, nil
}
