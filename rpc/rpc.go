package rpc

import (
	"context"
	"math/rand"
	"time"

	prototypes "github.com/gogo/protobuf/types"
	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	pstreamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
)

func (s *Server) GetPendingTask(ctx context.Context, req *v1.TaskPendingRequest) (*v1.Task, error) {
	span := opentracing.SpanFromContext(ctx)
	span.SetTag("client_id", req.ClientID)

	miner, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		span.SetTag("error", true)
		span.LogKV("event", "failed to authenticate", "message", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	span.SetTag("miner_id", miner.Id)
	span.SetTag("miner_status", miner.Status.String())
	span.SetTag("miner_user_id", miner.UserID)
	span.SetTag("miner_address", miner.Address)
	span.SetTag("miner_tags", miner.Tags)
	if miner.CapacityInfo != nil {
		span.SetTag("miner_capacity_info_encode", miner.CapacityInfo.Encode)
		span.SetTag("miner_capacity_info_cpu", miner.CapacityInfo.Cpu)
	}

	task, err := s.getPendingTask(ctx, miner)
	if err != nil {
		span.SetTag("error", true)
		span.LogKV("event", "no task", "message", err)
		return &v1.Task{}, nil
	}

	if task == nil {
		span.LogKV("event", "no task")
		return &v1.Task{}, nil
	}

	span.SetTag("task_id", task.ID)
	if task.Input != nil {
		span.SetTag("input", task.Input.URI)
	}
	span.SetTag("created_at", task.CreatedAt)
	span.SetTag("cmdline", task.Cmdline)
	span.SetTag("user_id", task.UserID.String)
	span.SetTag("stream_id", task.StreamID)
	span.SetTag("stream_contract_id", task.StreamContractID.Int64)
	span.SetTag("stream_contract_address", task.StreamContractAddress.String)
	span.SetTag("chunk_num", task.Output.Num)
	span.SetTag("profile_id", task.ProfileID)

	if task.IsOutputFile() {
		span.SetTag("vod", true)

		profile, err := s.dm.GetProfile(ctx, task.ProfileID)
		if err != nil {
			span.SetTag("error", true)
			span.LogKV("event", "failed to get profile", "message", err)
			return &v1.Task{}, nil
		}

		reward := profile.Cost / 60 * task.Output.Duration

		span.SetTag("reward", reward)

		achReq := &emitterv1.AddInputChunkRequest{
			StreamContractId: uint64(task.StreamContractID.Int64),
			ChunkId:          uint64(task.Output.Num),
			Reward:           reward,
		}
		_, err = s.emitter.AddInputChunk(ctx, achReq)
		if err != nil {
			span.SetTag("error", true)
			span.LogKV("event", "failed to add input chunk", "message", err)
			return &v1.Task{}, nil
		}
	} else {
		span.SetTag("live", true)
	}

	span.LogKV("event", "assigning task")

	err = s.assignTask(ctx, task, miner)
	if err != nil {
		span.SetTag("error", true)
		span.LogKV("event", "failed to assign task", "message", err)
		return &v1.Task{}, nil
	}

	return toTaskResponse(task), nil
}

func (s *Server) GetTask(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
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

func (s *Server) MarkTaskAsCompleted(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	miner, err := s.authenticate(ctx, req.ClientID)
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

	if task.Status < v1.TaskStatusCompleted {
		err = s.dm.MarkTaskAsCompleted(ctx, task)
		if err != nil {
			logFailedTo(s.logger, "mark task as completed", err)
			return nil, rpc.ErrRpcInternal
		}

		err := s.eb.EmitTaskCompleted(context.Background(), task, miner)
		if err != nil {
			logFailedTo(s.logger, "emit task completed", err)
		}

		go s.markStreamAsCompletedIfNeeded(task)
	}

	return toTaskResponse(task), nil
}

func (s *Server) MarkTaskAsFailed(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	_, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
	}

	if task.Status < v1.TaskStatusCompleted {
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
	}

	return toTaskResponse(task), nil
}

func (s *Server) ValidateProof(ctx context.Context, req *validatorv1.ValidateProofRequest) (*prototypes.Empty, error) {
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

func (s *Server) Ping(ctx context.Context, req *minersv1.PingRequest) (*minersv1.PingResponse, error) {
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

func (s *Server) Register(ctx context.Context, req *minersv1.RegistrationRequest) (*prototypes.Empty, error) {
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

	return new(prototypes.Empty), nil
}

func (s *Server) GetInternalConfig(ctx context.Context, req *v1.InternalConfigRequest) (*v1.InternalConfigResponse, error) {
	resp := &v1.InternalConfigResponse{}

	miners, err := s.miners.All(context.Background(), &prototypes.Empty{})
	if err != nil {
		return nil, err
	}

	excludeClientIds := []string{}
	if miners != nil && len(miners.Items) > 0 {
		for _, miner := range miners.Items {
			if miner.Status == minersv1.MinerStatusIdle ||
				miner.Status == minersv1.MinerStatusBusy {
				excludeClientIds = append(excludeClientIds, miner.Id)
			}
		}
	}

	clientIds, err := s.consul.GetTranscoderClientIds()
	if err != nil {
		return nil, err
	}

	if len(clientIds) > 0 {
		for {
			rand.Seed(time.Now().Unix())
			found := false
			resp.ClientId = clientIds[rand.Intn(len(clientIds))]
			for _, exludeClientID := range excludeClientIds {
				if resp.ClientId == exludeClientID {
					found = true
					break
				}
			}
			if !found {
				break
			}
		}

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
					_, err := s.miners.UnassignTask(
						context.Background(),
						&minersv1.AssignTaskRequest{TaskID: minerResp.TaskId},
					)
					s.logger.
						WithField("task_id", minerResp.TaskId).
						Errorf("failed to unassign task: %s", err)
				}()

				continue
			}

			if task.Status == v1.TaskStatusCompleted ||
				task.Status == v1.TaskStatusFailed ||
				task.Status == v1.TaskStatusCanceled {
				go func() {
					_, err := s.miners.UnassignTask(
						context.Background(),
						&minersv1.AssignTaskRequest{TaskID: minerResp.TaskId},
					)
					s.logger.
						WithField("task_id", minerResp.TaskId).
						Errorf("failed to unassign task: %s", err)
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

func (s *Server) GetConfig(ctx context.Context, req *v1.ConfigRequest) (*v1.ConfigResponse, error) {
	return &v1.ConfigResponse{
		RPCNodeURL: s.rpcNodeURL,
		SyncerURL:  s.syncerURL,
	}, nil
}

func (s *Server) MarkSegmentAsTranscoded(ctx context.Context, req *v1.TaskSegmentRequest) (*prototypes.Empty, error) {
	miner, err := s.authenticate(ctx, req.ClientID)
	if err != nil {
		s.logger.Warningf("failed to auth: %s", err)
		return nil, rpc.ErrRpcUnauthenticated
	}

	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
	}

	go func() {
		err := s.eb.EmitSegmentTranscoded(context.Background(), req, task, miner)
		if err != nil {
			logFailedTo(s.logger, "emit segment transcoded", err)
		}
	}()

	return new(prototypes.Empty), nil
}
