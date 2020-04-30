package rpc

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	"time"

	prototypes "github.com/gogo/protobuf/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/mailru/dbr"
	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	validatorv1 "github.com/videocoin/cloud-api/validator/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
)

func (s *Server) GetPendingTask(ctx context.Context, req *v1.TaskPendingRequest) (*v1.Task, error) {
	miner, _ := MinerFromContext(ctx)

	span := opentracing.SpanFromContext(ctx)
	minerResponseToSpan(span, miner)
	logger := s.logger.WithField("miner_id", miner.Id)

	task, err := s.getPendingTask(ctx, miner)
	if err != nil {
		spanErr(span, err, "no task")
		return &v1.Task{}, nil
	}
	if task == nil {
		span.LogKV("event", "no task")
		return &v1.Task{}, nil
	}

	logger = logger.WithField("task_id", task.ID)

	otCtx := opentracing.ContextWithSpan(context.Background(), span)

	defer func() {
		err := s.dm.UnlockTask(otCtx, task)
		if err != nil {
			logger.WithField("task_id", task.ID).Error("failed to unlock task")
		}
	}()

	taskToSpan(span, task)

	if task.IsOutputFile() {
		if task.Status == v1.TaskStatusPending {
			profile, err := s.dm.GetProfile(otCtx, task.ProfileID)
			if err != nil {
				errMsg := "failed to get profile"
				spanErr(span, err, errMsg)
				logger.WithError(err).Error(errMsg)

				return &v1.Task{}, nil
			}

			reward := profile.Cost / 60 * task.Output.Duration

			span.SetTag("reward", reward)

			achReq := &emitterv1.AddInputChunkRequest{
				StreamContractId: uint64(task.StreamContractID.Int64),
				ChunkId:          uint64(task.Output.Num),
				Reward:           reward,
			}
			aicResp, err := s.sc.Emitter.AddInputChunk(otCtx, achReq)
			if err != nil {
				if aicResp != nil {
					logger = logger.WithFields(logrus.Fields{
						"tx":        aicResp.Tx,
						"tx_status": aicResp.Status,
					})
				}

				errMsg := "failed to add input chunk"
				spanErr(span, err, errMsg)
				logger.WithError(err).Error(errMsg)

				s.markTaskAsFailed(otCtx, task)

				return &v1.Task{}, nil
			}

			err = s.eb.EmitAddInputChunk(otCtx, task, miner)
			if err != nil {
				logger.WithError(err).Error("failed to emit add input chunk")
			}

			taskTx := &datastore.TaskTx{
				TaskID:                task.ID,
				StreamContractID:      strconv.FormatInt(task.StreamContractID.Int64, 10),
				StreamContractAddress: task.StreamContractAddress.String,
				ChunkID:               task.Output.Num,
				AddInputChunkTx:       dbr.NewNullString(aicResp.Tx),
				AddInputChunkTxStatus: dbr.NewNullString(aicResp.Status.String()),
			}
			err = s.dm.CreateTaskTx(otCtx, taskTx)
			if err != nil {
				errMsg := "failed to create task tx"
				spanErr(span, err, errMsg)
				logger.WithError(err).Error(errMsg)

				s.markTaskAsFailed(otCtx, task)

				return &v1.Task{}, nil
			}
		}
	}

	span.LogKV("event", "assigning task")

	err = s.assignTask(otCtx, task, miner)
	if err != nil {
		errMsg := "failed to assign task"
		spanErr(span, err, errMsg)
		logger.WithError(err).Error(errMsg)

		return &v1.Task{}, nil
	}

	return toTaskResponse(task), nil
}

func (s *Server) GetTask(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
	}

	return toTaskResponse(task), nil
}

func (s *Server) MarkTaskAsCompleted(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	miner, _ := MinerFromContext(ctx)
	span := opentracing.SpanFromContext(ctx)
	minerResponseToSpan(span, miner)

	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
	}

	taskToSpan(span, task)

	logger := s.logger.WithField("task_id", task.ID)
	logger.Info("marking task as completed")

	otCtx := opentracing.ContextWithSpan(context.Background(), span)

	defer func() {
		atReq := &minersv1.AssignTaskRequest{
			ClientID: task.ClientID.String,
			TaskID:   task.ID,
		}
		_, err = s.sc.Miners.UnassignTask(otCtx, atReq)
		if err != nil {
			logger.WithError(err).Error("failed to unassign task to miners service")
		}
	}()

	if task.Status < v1.TaskStatusCompleted || task.Status == v1.TaskStatusPaused {
		err = s.dm.MarkTaskAsCompleted(otCtx, task)
		if err != nil {
			logger.WithError(err).Error("failed to mark task as completed")
			return nil, rpc.ErrRpcInternal
		}

		s.markStreamAsCompletedIfNeeded(ctxlogrus.ToContext(otCtx, logger), task)
	}

	return toTaskResponse(task), nil
}

func (s *Server) MarkTaskAsFailed(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	miner, _ := MinerFromContext(ctx)
	span := opentracing.SpanFromContext(ctx)
	minerResponseToSpan(span, miner)

	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
	}

	taskToSpan(span, task)

	if task.Status == v1.TaskStatusCompleted ||
		task.Status == v1.TaskStatusFailed ||
		task.Status == v1.TaskStatusCanceled {
		return toTaskResponse(task), nil
	}

	otCtx := opentracing.ContextWithSpan(context.Background(), span)
	s.markTaskAsFailed(otCtx, task)

	atReq := &minersv1.AssignTaskRequest{
		ClientID: task.ClientID.String,
		TaskID:   task.ID,
	}
	_, err = s.sc.Miners.UnassignTask(otCtx, atReq)
	if err != nil {
		s.logger.WithError(err).Error("failed to unassign task")
	}

	return toTaskResponse(task), nil
}

func (s *Server) MarkTaskAsPaused(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	miner, _ := MinerFromContext(ctx)
	span := opentracing.SpanFromContext(ctx)
	minerResponseToSpan(span, miner)

	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
	}

	taskToSpan(span, task)

	if task.Status == v1.TaskStatusCompleted ||
		task.Status == v1.TaskStatusFailed ||
		task.Status == v1.TaskStatusCanceled ||
		task.Status == v1.TaskStatusPaused {
		return toTaskResponse(task), nil
	}

	logger := s.logger.WithField("task_id", task.ID)
	otCtx := opentracing.ContextWithSpan(context.Background(), span)

	if task.IsOutputHLS() {
		logger.Info("marking task as completed (paused)")

		err = s.dm.MarkTaskAsCompleted(otCtx, task)
		if err != nil {
			logger.WithError(err).Error("failed to mark task as completed")
			return nil, rpc.ErrRpcInternal
		}

		s.markStreamAsCompletedIfNeeded(ctxlogrus.ToContext(otCtx, logger), task)
		return toTaskResponse(task), nil
	}

	logger.Info("marking task as paused")

	err = s.dm.MarkTaskAsPaused(otCtx, task)
	if err != nil {
		logger.WithError(err).Error("failed to mark task as paused")
		return nil, rpc.ErrRpcInternal
	}

	return toTaskResponse(task), nil
}

func (s *Server) MarkSegmentAsTranscoded(ctx context.Context, req *v1.TaskSegmentRequest) (*prototypes.Empty, error) {
	miner, _ := MinerFromContext(ctx)
	span := opentracing.SpanFromContext(ctx)
	minerResponseToSpan(span, miner)

	task, err := s.getTask(req.ID)
	if err != nil {
		return nil, err
	}

	taskToSpan(span, task)
	otCtx := opentracing.ContextWithSpan(context.Background(), span)

	go func() {
		err := s.eb.EmitSegmentTranscoded(otCtx, req, task, miner)
		if err != nil {
			s.logger.WithError(err).Error("failed to emit segment transcoded")
		}
	}()

	return new(prototypes.Empty), nil
}

func (s *Server) ValidateProof(ctx context.Context, req *validatorv1.ValidateProofRequest) (*validatorv1.ValidateProofResponse, error) {
	miner, _ := MinerFromContext(ctx)
	span := opentracing.SpanFromContext(ctx)
	minerResponseToSpan(span, miner)

	chunkID := new(big.Int).SetBytes(req.ChunkId).Int64()
	profileID := new(big.Int).SetBytes(req.ProfileId).Int64()

	span.SetTag("stream_contract_address", req.StreamContractAddress)
	span.SetTag("chunk_id", chunkID)
	span.SetTag("profile_id", profileID)

	logger := s.logger.WithFields(logrus.Fields{
		"stream_contract_address": req.StreamContractAddress,
		"chunk_id":                chunkID,
		"profile_id":              profileID,
	})

	logger.Info("validating proof")

	data := datastore.UpdateProof{
		StreamContractAddress: req.StreamContractAddress,
		ChunkID:               chunkID,
		ProfileID:             profileID,
		SubmitProofTx:         req.SubmitProofTx,
		SubmitProofTxStatus:   req.SubmitProofTxStatus,
	}

	otCtx := opentracing.ContextWithSpan(context.Background(), span)
	resp, _ := s.sc.Validator.ValidateProof(otCtx, req)
	if resp != nil {
		data.ValidateProofTx = resp.ValidateProofTx
		data.ValidateProofTxStatus = resp.ValidateProofTxStatus
		data.ScrapProofTx = resp.ScrapProofTx
		data.ScrapProofTxStatus = resp.ScrapProofTxStatus
	}
	upErr := s.dm.UpdateProof(ctxlogrus.ToContext(otCtx, logger), data)
	if upErr != nil {
		logger.WithError(upErr).Error("failed to update proof")
	}

	return resp, nil
}

func (s *Server) Ping(ctx context.Context, req *minersv1.PingRequest) (*minersv1.PingResponse, error) {
	go func(req *minersv1.PingRequest) {
		_, _ = s.sc.Miners.Ping(context.Background(), req)
	}(req)

	return &minersv1.PingResponse{}, nil
}

func (s *Server) Register(ctx context.Context, req *minersv1.RegistrationRequest) (*prototypes.Empty, error) {
	_, err := s.sc.Miners.Register(ctx, req)
	if err != nil {
		return nil, err
	}

	return new(prototypes.Empty), nil
}

func (s *Server) GetInternalConfig(ctx context.Context, req *v1.InternalConfigRequest) (*v1.InternalConfigResponse, error) {
	resp := &v1.InternalConfigResponse{}

	miners, err := s.sc.Miners.All(context.Background(), &prototypes.Empty{})
	if err != nil {
		return nil, err
	}

	excludeClientIds := []string{}
	if miners != nil && len(miners.Items) > 0 {
		for _, miner := range miners.Items {
			if miner.Status == minersv1.MinerStatusIdle ||
				miner.Status == minersv1.MinerStatusBusy ||
				miner.UserID != "6b7a9f95-b6e6-4609-55f2-da4aef36184e" {
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

	minersResp, _ := s.sc.Miners.GetMinersWithForceTask(context.Background(), &prototypes.Empty{})
	if minersResp != nil {
		for _, minerResp := range minersResp.Items {
			task, err := s.dm.GetTaskByID(context.Background(), minerResp.TaskId)
			if err == datastore.ErrTaskNotFound {
				go func() {
					_, err := s.sc.Miners.UnassignTask(
						context.Background(),
						&minersv1.AssignTaskRequest{TaskID: minerResp.TaskId},
					)
					s.logger.
						WithError(err).
						WithField("task_id", minerResp.TaskId).
						Error("failed to unassign task")
				}()

				continue
			}

			if task.Status == v1.TaskStatusCompleted ||
				task.Status == v1.TaskStatusFailed ||
				task.Status == v1.TaskStatusCanceled {
				go func() {
					_, err := s.sc.Miners.UnassignTask(
						context.Background(),
						&minersv1.AssignTaskRequest{TaskID: minerResp.TaskId},
					)
					s.logger.
						WithError(err).
						WithField("task_id", minerResp.TaskId).
						Error("failed to unassign task")
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

func (s *Server) AddInputChunk(ctx context.Context, req *v1.AddInputChunkRequest) (*v1.AddInputChunkResponse, error) {
	span := opentracing.SpanFromContext(ctx)
	span.SetTag("stream_id", req.StreamId)
	span.SetTag("stream_contract_id", req.StreamContractId)
	span.SetTag("chunk_id", req.ChunkId)
	span.SetTag("reward", req.Reward)

	logger := s.logger.WithFields(logrus.Fields{
		"stream_id":          req.StreamId,
		"stream_contract_id": req.StreamContractId,
		"chunk_id":           req.ChunkId,
		"reward":             req.Reward,
	})

	logger.Info("add input chunk")

	aicReq := &emitterv1.AddInputChunkRequest{
		StreamContractId: req.StreamContractId,
		ChunkId:          req.ChunkId,
		Reward:           req.Reward,
	}

	otCtx := opentracing.ContextWithSpan(context.Background(), span)
	aicResp, err := s.sc.Emitter.AddInputChunk(otCtx, aicReq)
	if err != nil {
		if aicResp != nil {
			logger = logger.WithField("tx", aicResp.Tx)
		}
		logger.WithError(err).Error("failed to add input chunk")
		fmtErr := fmt.Errorf("failed to Emitter.AddInputChunk: %s", err)
		return nil, rpc.NewRpcInternalError(fmtErr)
	}

	resp := &v1.AddInputChunkResponse{
		Tx:     aicResp.Tx,
		Status: aicResp.Status,
	}

	logger = logger.WithFields(logrus.Fields{
		"tx":        resp.Tx,
		"tx_status": resp.Status.String(),
	})
	logger.Info("add input chunk successful")

	if aicResp != nil {
		data := datastore.AddInputChunk{
			StreamID:              req.StreamId,
			StreamContractID:      strconv.FormatUint(req.StreamContractId, 10),
			ChunkID:               int64(req.ChunkId),
			AddInputChunkTx:       resp.Tx,
			AddInputChunkTxStatus: resp.Status,
		}
		err := s.dm.AddInputChunk(ctxlogrus.ToContext(otCtx, logger), data)
		if err != nil {
			logger.WithError(err).Error("failed to dm.AddInputChunk")
			fmtErr := fmt.Errorf("failed to dm.AddInputChunk: %s", err)
			return resp, rpc.NewRpcInternalError(fmtErr)
		}
	}

	return resp, nil
}
