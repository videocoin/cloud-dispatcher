package rpc

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	"github.com/videocoin/cloud-api/rpc"
	pstreamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"go.uber.org/zap"
)

func (s *Server) getTask(id string) (*datastore.Task, error) {
	task, err := s.dm.GetTaskByID(context.Background(), id)
	if err != nil {
		s.logger.Error("failed to get task", zap.Error(err))
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	return task, nil
}

func (s *Server) markStreamAsCompletedIfNeeded(ctx context.Context, task *datastore.Task) {
	logger := ctxzap.Extract(ctx).With(zap.String("stream_id", task.StreamID))

	if task.ID != task.StreamID {
		logger.Info("getting tasks by stream")

		relTasks, err := s.dm.GetTasksByStreamID(ctx, task.StreamID)
		if err != nil {
			logger.Error("failed to get tasks by stream", zap.Error(err))
			return
		}

		relTasksCount := len(relTasks)
		relCompletedTasksCount := 0

		for _, t := range relTasks {
			if t.Status == v1.TaskStatusCompleted {
				relCompletedTasksCount++
			}
		}

		if relTasksCount == relCompletedTasksCount {
			logger.Info("complete stream")

			_, err := s.sc.Streams.Complete(ctx, &pstreamsv1.StreamRequest{Id: task.StreamID})
			if err != nil {
				logger.Error("failed to complete stream", zap.Error(err))
				return
			}
		}
	} else {
		logger.Info("complete stream")

		_, err := s.sc.Streams.Complete(ctx, &pstreamsv1.StreamRequest{Id: task.StreamID})
		if err != nil {
			logger.Error("failed to complete stream", zap.Error(err))
			return
		}
	}
}

func (s *Server) markStreamAsFailedIfNeeded(ctx context.Context, task *datastore.Task) {
	logger := s.logger.With(
		zap.String("id", task.ID),
		zap.String("stream_id", task.StreamID),
	)

	if task.ID != task.StreamID {
		logger.Info("getting tasks by stream")
		relTasks, err := s.dm.GetTasksByStreamID(ctx, task.StreamID)
		if err != nil {
			logger.Error("get tasks by stream", zap.Error(err))
		} else {
			for _, relTask := range relTasks {
				if relTask.ID != task.ID {
					if relTask.Status == v1.TaskStatusCreated ||
						relTask.Status == v1.TaskStatusPending ||
						relTask.Status == v1.TaskStatusAssigned ||
						relTask.Status == v1.TaskStatusEncoding {
						err := s.dm.MarkTaskAsCanceled(ctx, relTask)
						if err != nil {
							logger.Error("failed to mark task as canceled", zap.Error(err))
						}
					}
				}
			}
		}
	}

	updateReq := &pstreamsv1.UpdateStatusRequest{
		ID:     task.StreamID,
		Status: streamsv1.StreamStatusFailed,
	}
	_, err := s.sc.Streams.UpdateStatus(ctx, updateReq)
	if err != nil {
		logger.Error("failed to update stream status", zap.Error(err))
		return
	}
}

func (s *Server) markTaskAsRetryable(task *datastore.Task) bool {
	isRetryable := false
	ctx := context.Background()
	taskLog, err := s.dm.GetTaskLog(ctx, task.ID)
	if err == nil {
		taskLogCount := len(taskLog)
		if taskLogCount < 2 {
			err := s.dm.MarkTaskAsPending(ctx, task)
			if err != nil {
				s.logger.Error("mark task as pending (failed)", zap.Error(err))
			} else {
				err := s.dm.ClearClientID(ctx, task)
				if err != nil {
					s.logger.
						With(zap.String("task_id", task.ID)).
						Error("failed to clear client id", zap.Error(err))
				}
				isRetryable = true
			}
		}
	}

	return isRetryable
}

// func (s *Server) isMinerQualify(ctx context.Context, miner *minersv1.MinerResponse, task *datastore.Task) (bool, error) {
// 	resp, err := s.miners.GetMinersCandidates(ctx, &minersv1.MinersCandidatesRequest{
// 		EncodeCapacity: task.Capacity.Encode,
// 		CpuCapacity:    task.Capacity.Cpu,
// 	})
// 	if err != nil {
// 		return false, err
// 	}

// 	miners := resp.Items
// 	if len(miners) <= 1 {
// 		return true, nil
// 	}

// 	sort.Slice(miners[:], func(i, j int) bool {
// 		return miners[i].Stake > miners[j].Stake
// 	})

// 	var qStake int32
// 	for _, m := range miners {
// 		qStake += m.Stake
// 	}

// 	rand.Seed(time.Now().UnixNano())
// 	r := rand.Float64()

// 	var choosenMinerID string
// 	for _, m := range miners {
// 		weight := float64(m.Stake) / float64(qStake)
// 		if weight > r {
// 			choosenMinerID = m.ID
// 			break
// 		} else {
// 			r = r - weight
// 		}
// 	}

// 	if choosenMinerID != miner.Id {
// 		return false, nil
// 	}

// 	return true, nil
// }
