package rpc

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	emitterv1 "github.com/videocoin/cloud-api/emitter/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	pstreamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
)

func (s *Server) getTask(id string) (*datastore.Task, error) {
	task, err := s.dm.GetTaskByID(context.Background(), id)
	if err != nil {
		s.logger.WithError(err).Error("failed to get task")
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	return task, nil
}

func (s *Server) markStreamAsCompletedIfNeeded(ctx context.Context, task *datastore.Task) {
	logger := ctxlogrus.Extract(ctx).WithField("stream_id", task.StreamID)

	if task.ID != task.StreamID {
		logger.Info("getting tasks by stream")

		relTasks, err := s.dm.GetTasksByStreamID(ctx, task.StreamID)
		if err != nil {
			logger.WithError(err).Error("failed to get tasks by stream")
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
				logger.WithError(err).Error("failed to complete stream")
				return
			}
		}
	} else {
		logger.Info("complete stream")

		_, err := s.sc.Streams.Complete(ctx, &pstreamsv1.StreamRequest{Id: task.StreamID})
		if err != nil {
			logger.WithError(err).Error("failed to complete stream")
			return
		}
	}
}

func (s *Server) markStreamAsFailedIfNeeded(ctx context.Context, task *datastore.Task) {
	logger := s.logger.WithFields(logrus.Fields{
		"task_id":   task.ID,
		"stream_id": task.StreamID,
	})

	if task.ID != task.StreamID {
		logger.Info("getting tasks by stream")
		relTasks, err := s.dm.GetTasksByStreamID(ctx, task.StreamID)
		if err != nil {
			logger.WithError(err).Error("get tasks by stream")
		} else {
			for _, relTask := range relTasks {
				if relTask.ID != task.ID {
					if relTask.Status == v1.TaskStatusCreated ||
						relTask.Status == v1.TaskStatusPending ||
						relTask.Status == v1.TaskStatusAssigned ||
						relTask.Status == v1.TaskStatusEncoding {

						go func() {
							atReq := &minersv1.AssignTaskRequest{
								ClientID: relTask.ClientID.String,
								TaskID:   relTask.ID,
							}
							_, err = s.sc.Miners.UnassignTask(ctx, atReq)
							if err != nil {
								logger.WithError(err).Error("failed to unassign task to miners service")
							}
						}()

						err := s.dm.MarkTaskAsCanceled(ctx, relTask)
						if err != nil {
							logger.WithError(err).Error("failed to mark task as canceled")
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
		logger.WithError(err).Error("failed to update stream status")
		return
	}
}

func (s *Server) checkWorkerState(ctx context.Context, miner *minersv1.MinerResponse) error {
	if miner.Address == "" {
		return ErrWorkerAddressIsEmpty
	}

	req := &emitterv1.WorkerRequest{Address: miner.Address}
	worker, err := s.sc.Emitter.GetWorker(ctx, req)
	if err != nil {
		return err
	}

	if worker.State != emitterv1.WorkerStateBonded {
		switch worker.State {
		case emitterv1.WorkerStateBonding:
			return ErrWorkerStateIsBonding
		case emitterv1.WorkerStateUnbonded:
			return ErrWorkerStateIsUnbonded
		case emitterv1.WorkerStateUnbonding:
			return ErrWorkerStateIsUnbonding
		default:
			return ErrWrongWorkerState
		}
	}

	return nil
}
