package rpc

import (
	"context"
	"errors"

	prototypes "github.com/gogo/protobuf/types"
	"github.com/jinzhu/copier"
	"github.com/mailru/dbr"
	"github.com/sirupsen/logrus"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	syncerv1 "github.com/videocoin/cloud-api/syncer/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var (
	ErrClientIDIsEmpty  = errors.New("client id is empty")
	ErrClientIDNotFounf = errors.New("client id not found")
)

func (s *RpcServer) authenticate(ctx context.Context, clientID string) (*minersv1.MinerResponse, error) {
	if clientID == "" {
		return nil, ErrClientIDIsEmpty
	}

	miner, err := s.miners.Get(context.Background(), &minersv1.MinerRequest{Id: clientID})
	if err != nil {
		return nil, err
	}

	if miner == nil {
		return nil, ErrClientIDNotFounf
	}

	return miner, nil
}

func (s *RpcServer) GetPendingTask(ctx context.Context, req *v1.TaskPendingRequest) (*v1.Task, error) {
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

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

	atReq := &minersv1.AssignTaskRequest{
		ClientID: task.ClientID.String,
		TaskID:   task.ID,
	}
	_, err = s.miners.AssignTask(context.Background(), atReq)
	if err != nil {
		logFailedTo(s.logger, "assign task to miners service", err)
	}

	return v1Task, nil
}

func (s *RpcServer) GetTask(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

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
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	task, err := s.dm.GetTaskByID(ctx, req.ID)
	if err != nil {
		logFailedTo(s.logger, "get task", err)
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	defer func() {
		atReq := &minersv1.AssignTaskRequest{
			ClientID: task.ClientID.String,
			TaskID:   task.ID,
		}
		_, err = s.miners.UnassignTask(context.Background(), atReq)
		if err != nil {
			logFailedTo(s.logger, "unassign task to miners service", err)
		}
	}()

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
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	task, err := s.dm.GetTaskByID(ctx, req.ID)
	if err != nil {
		logFailedTo(s.logger, "get task", err)
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	defer func() {
		atReq := &minersv1.AssignTaskRequest{
			ClientID: task.ClientID.String,
			TaskID:   task.ID,
		}
		_, err = s.miners.UnassignTask(context.Background(), atReq)
		if err != nil {
			logFailedTo(s.logger, "unassign task to miners service", err)
		}
	}()

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
	logger := s.logger.WithFields(logrus.Fields{
		"object_name": req.Path,
	})

	logger.Info("syncing")

	go func(logger *logrus.Entry, req *syncerv1.SyncRequest) {
		_, err := s.syncer.Sync(context.Background(), req)
		if err != nil {
			logger.Errorf("failed to sync: %s", err)
		}
	}(logger, req)

	return &prototypes.Empty{}, nil
}

func (s *RpcServer) Ping(
	ctx context.Context,
	req *minersv1.PingRequest,
) (*minersv1.PingResponse, error) {
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

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

func (s *RpcServer) Register(
	ctx context.Context,
	req *v1.RegistrationRequest,
) (*prototypes.Empty, error) {
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	logger := s.logger.WithFields(logrus.Fields{
		"client_id": req.ClientID,
	})

	logger.Info("registering")

	_, err = s.miners.Ping(context.Background(), &minersv1.PingRequest{ClientID: req.ClientID})
	if err != nil {
		s.logger.Errorf("failed to ping registration: %s", err)
		return nil, rpc.ErrRpcInternal
	}

	miner, err := s.miners.Get(context.Background(), &minersv1.MinerRequest{Id: req.ClientID})
	if err != nil {
		s.logger.Warningf("failed to get miner: %s", err)
		return nil, rpc.ErrRpcNotFound
	}

	if miner.Status != minersv1.MinerStatusOffline {
		logger.Warningf("miner is already running")
		return nil, grpc.Errorf(codes.AlreadyExists, "miner is already running")
	}

	return &prototypes.Empty{}, nil
}
