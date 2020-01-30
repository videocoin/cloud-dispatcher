package rpc

import (
	"context"
	"errors"
	"math/rand"
	"time"

	prototypes "github.com/gogo/protobuf/types"
	"github.com/jinzhu/copier"
	"github.com/mailru/dbr"
	"github.com/sirupsen/logrus"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	syncerv1 "github.com/videocoin/cloud-api/syncer/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
)

var (
	ErrClientIDIsEmpty  = errors.New("client id is empty")
	ErrClientIDNotFound = errors.New("client id not found")
)

func (s *RpcServer) authenticate(ctx context.Context, clientID string) (*minersv1.MinerResponse, error) {
	if clientID == "" {
		return nil, ErrClientIDIsEmpty
	}

	miner, err := s.miners.GetByID(context.Background(), &minersv1.MinerRequest{Id: clientID})
	if err != nil {
		return nil, err
	}

	if miner == nil {
		return nil, ErrClientIDNotFound
	}

	return miner, nil
}

func (s *RpcServer) GetPendingTask(ctx context.Context, req *v1.TaskPendingRequest) (*v1.Task, error) {
	miner, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	logger := s.logger.WithField("client_id", req.ClientID)

	task := &datastore.Task{}
	task = nil

	if forceTaskID, ok := miner.Tags["force_task_id"]; ok {
		task, err = s.dm.GetPendingTaskByID(ctx, forceTaskID)
		if err != nil {
			logFailedTo(logger, "get force task", err)
			return nil, rpc.ErrRpcInternal
		}
		if task.Status != v1.TaskStatusPending {
			task = nil
		}
	}

	if task == nil {
		ft, err := s.miners.GetForceTaskList(ctx, &prototypes.Empty{})
		if err != nil {
			logFailedTo(logger, "get force task ids", err)
			return nil, rpc.ErrRpcNotFound
		}

		task, err = s.dm.GetPendingTask(ctx, ft.Ids)
		if err != nil {
			logFailedTo(s.logger, "get pending task", err)
			return nil, rpc.ErrRpcInternal
		}
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
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

	task.ClientID = dbr.NewNullString(req.ClientID)
	err = s.dm.MarkTaskAsAssigned(ctx, task)
	if err != nil {
		logFailedTo(logger, "mark as assigned", err)
		return nil, rpc.ErrRpcInternal
	}

	logger.WithField("assigned_client_id", task.ClientID.String).Info("task has been assinged")

	v1Task := &v1.Task{}
	err = copier.Copy(v1Task, task)
	if err != nil {
		logFailedTo(s.logger, "copy task", err)
		return nil, rpc.ErrRpcInternal
	}

	v1Task.ClientID = task.ClientID.String
	v1Task.StreamContractID = uint64(task.StreamContractID.Int64)
	v1Task.StreamContractAddress = task.StreamContractAddress.String
	v1Task.MachineType = task.MachineType.String
	v1Task.StreamID = task.StreamID

	atReq := &minersv1.AssignTaskRequest{
		ClientID: task.ClientID.String,
		TaskID:   task.ID,
	}
	_, err = s.miners.AssignTask(context.Background(), atReq)
	if err != nil {
		logFailedTo(logger, "assign task to miners service", err)
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
	v1Task.MachineType = task.MachineType.String
	v1Task.StreamID = task.StreamID

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

		_, err = s.streams.PublishDone(
			context.Background(),
			&streamsv1.StreamRequest{Id: task.StreamID},
		)
		if err != nil {
			logFailedTo(s.logger, "publish done", err)
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
	v1Task.StreamID = task.StreamID

	go func() {
		if task.ID != task.StreamID {
			logger := s.logger.
				WithField("id", task.ID).
				WithField("stream_id", task.StreamID)
			logger.Info("getting tasks by stream")
			relTasks, err := s.dm.GetTasksByStreamID(ctx, task.StreamID)
			if err != nil {
				logFailedTo(s.logger, "get tasks by stream", err)
				return
			}

			relTasksCount := len(relTasks)
			relCompletedTasksCount := 0

			logger.Infof("relation tasks count - %d", relTasksCount)

			for _, t := range relTasks {
				if t.Status == v1.TaskStatusCompleted {
					relCompletedTasksCount++
				}
			}

			logger.Infof("relation completed tasks count - %d", relCompletedTasksCount)

			if relTasksCount == relCompletedTasksCount {
				logger.Infof("publish done")

				_, err := s.streams.PublishDone(context.Background(), &streamsv1.StreamRequest{Id: task.StreamID})
				if err != nil {
					logFailedTo(s.logger, "file publish done", err)
					return
				}
			}
		}

	}()

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

		_, err = s.streams.PublishDone(
			context.Background(),
			&streamsv1.StreamRequest{Id: task.StreamID},
		)
		if err != nil {
			logFailedTo(s.logger, "publish done", err)
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
	v1Task.StreamID = task.StreamID

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
	req *minersv1.RegistrationRequest,
) (*prototypes.Empty, error) {
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

func (s *RpcServer) GetInternalConfig(
	ctx context.Context,
	req *v1.InternalConfigRequest,
) (*v1.InternalConfigResponse, error) {
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
