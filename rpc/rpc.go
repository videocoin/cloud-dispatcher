package rpc

import (
	"context"
	"math/rand"
	"time"

	prototypes "github.com/gogo/protobuf/types"
	"github.com/sirupsen/logrus"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	pstreamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
)

func (s *RpcServer) GetPendingTask(ctx context.Context, req *v1.TaskPendingRequest) (*v1.Task, error) {
	miner, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	logger := s.logger.WithField("client_id", req.ClientID)

	task, err := s.getPendingTask(miner)
	if err != nil {
		return nil, err
	}

	if task.IsOutputFile() {
		logger = logger.WithFields(logrus.Fields{
			"stream_contract_id": task.StreamContractID.Int64,
			"chunk_id":           task.Output.Num,
		})

		achReq := &emitterv1.AddInputChunkIdRequest{
			StreamContractId: uint64(task.StreamContractID.Int64),
			ChunkId:          uint64(task.Output.Num),
			ChunkDuration:    10,
		}

		logger.Debugf("calling AddInputChunkId")

		_, err = s.emitter.AddInputChunkId(context.Background(), achReq)
		if err != nil {
			logger.Errorf("failed to add input chunk: %s", err.Error())
			return nil, rpc.ErrRpcNotFound
		}
	}

	err = s.assignTask(task, miner)
	if err != nil {
		return nil, err
	}

	return toTaskResponse(task), nil
}

func (s *RpcServer) GetTask(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
	}

	return toTaskResponse(task), nil
}

func (s *RpcServer) MarkTaskAsCompleted(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
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

	return toTaskResponse(task), nil
}

func (s *RpcServer) MarkTaskAsFailed(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
	}

	isRetryable := s.markTaskAsRetryable(task)

	defer func() {
		atReq := &minersv1.AssignTaskRequest{
			ClientID: task.ClientID.String,
			TaskID:   task.ID,
		}
		_, err = s.miners.UnassignTask(context.Background(), atReq)
		if err != nil {
			logFailedTo(s.logger, "unassign task to miners service", err)
		}

		if !isRetryable {
			_, err = s.streams.PublishDone(
				context.Background(),
				&pstreamsv1.StreamRequest{Id: task.StreamID},
			)
			if err != nil {
				logFailedTo(s.logger, "publish done", err)
			}
		}
	}()

	if !isRetryable {
		err = s.dm.MarkTaskAsFailed(ctx, task)
		if err != nil {
			logFailedTo(s.logger, "mark task as failed", err)
			return nil, rpc.ErrRpcInternal
		}

		go s.markStreamAsFailedIfNeeded(task)
	}

	return toTaskResponse(task), nil
}

func (s *RpcServer) ValidateProof(ctx context.Context, req *validatorv1.ValidateProofRequest) (*prototypes.Empty, error) {
	logger := s.logger.WithField("stream_id", req.StreamId)

	relTasks, err := s.dm.GetTasksByStreamID(ctx, req.StreamId)
	if err != nil {
		logFailedTo(s.logger, "get tasks by stream", err)
		return nil, err
	}

	relTasksCount := len(relTasks)
	relCompletedTasksCount := 0

	logger.Infof("relation tasks count - %d", relTasksCount)

	if relTasksCount > 1 {
		for _, t := range relTasks {
			if t.Status == v1.TaskStatusCompleted {
				relCompletedTasksCount++
			}
		}

		logger.Infof("relation completed tasks count - %d", relCompletedTasksCount)

		if relTasksCount == relCompletedTasksCount+1 {
			req.IsLast = true
		}
	}

	return s.validator.ValidateProof(ctx, req)
}

func (s *RpcServer) Ping(ctx context.Context, req *minersv1.PingRequest) (*minersv1.PingResponse, error) {
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

func (s *RpcServer) Register(ctx context.Context, req *minersv1.RegistrationRequest) (*prototypes.Empty, error) {
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	logger := s.logger.WithFields(logrus.Fields{
		"client_id": req.ClientID,
		"address":   req.Address,
	})

	logger.Info("registering")

	_, err = s.miners.Register(ctx, req)
	if err != nil {
		return nil, err
	}

	return &prototypes.Empty{}, nil
}

func (s *RpcServer) GetInternalConfig(ctx context.Context, req *v1.InternalConfigRequest) (*v1.InternalConfigResponse, error) {
	resp := &v1.InternalConfigResponse{}

	clientIds, err := s.consul.GetTranscoderClientIds()
	if err != nil {
		return nil, err
	}

	if len(clientIds) > 0 {
		rand.Seed(time.Now().Unix())
		resp.ClientId = clientIds[rand.Intn(len(clientIds))]
	}

	ksPairs, err := s.consul.GetTranscoderKeyAndSecret()
	if err != nil {
		return nil, err
	}

	if len(ksPairs) > 0 {
		rand.Seed(time.Now().Unix())
		ksPair := ksPairs[rand.Intn(len(ksPairs))]
		resp.Key = string(ksPair.Value)
		resp.Secret = ksPair.Key
	}

	minersResp, _ := s.miners.GetMinersWithForceTask(context.Background(), &prototypes.Empty{})
	if minersResp != nil {
		for _, minerResp := range minersResp.Items {
			task, err := s.dm.GetTaskByID(context.Background(), minerResp.TaskId)
			if err == nil && task == nil {
				go func() {
					s.miners.UnassignTask(
						context.Background(),
						&minersv1.AssignTaskRequest{TaskID: minerResp.TaskId},
					)
				}()

				continue
			}

			if task.Status == v1.TaskStatusCompleted ||
				task.Status == v1.TaskStatusFailed ||
				task.Status == v1.TaskStatusCanceled {
				go func() {
					s.miners.UnassignTask(
						context.Background(),
						&minersv1.AssignTaskRequest{TaskID: minerResp.TaskId},
					)
				}()

				continue
			}

			if task.Status == v1.TaskStatusPending {
				resp.ClientId = minerResp.Id
				break
			}
		}
	}

	return resp, nil
}

func (s *RpcServer) GetConfig(ctx context.Context, req *v1.ConfigRequest) (*v1.ConfigResponse, error) {
	return &v1.ConfigResponse{
		RPCNodeURL: s.rpcNodeURL,
		SyncerURL:  s.syncerURL,
	}, nil
}
