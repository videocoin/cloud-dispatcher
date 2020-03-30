package rpc

import (
	"context"
	"errors"
	"strings"

	"github.com/mailru/dbr"

	prototypes "github.com/gogo/protobuf/types"

	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	pstreamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
)

var (
	ErrClientIDIsEmpty  = errors.New("client id is empty")
	ErrClientIDNotFound = errors.New("client id not found")
)

func (s *Server) authenticate(ctx context.Context, clientID string) (*minersv1.MinerResponse, error) {
	if clientID == "" {
		return nil, ErrClientIDIsEmpty
	}

	miner, err := s.miners.GetByID(ctx, &minersv1.MinerRequest{Id: clientID})
	if err != nil {
		return nil, err
	}

	if miner == nil {
		return nil, ErrClientIDNotFound
	}

	return miner, nil
}

func (s *Server) getTask(id string) (*datastore.Task, error) {
	task, err := s.dm.GetTaskByID(context.Background(), id)
	if err != nil {
		logFailedTo(s.logger, "get task", err)
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	return task, nil
}

func (s *Server) getPendingTask(miner *minersv1.MinerResponse) (*datastore.Task, error) {
	var err error
	var task *datastore.Task

	ctx := context.Background()
	logger := s.logger.WithField("client_id", miner.Id)

	if forceTaskID, ok := miner.Tags["force_task_id"]; ok {
		logger.Info("getting force task")

		task, err = s.dm.GetPendingTaskByID(ctx, forceTaskID)
		if err != nil {
			logFailedTo(logger, "get force task", err)
			return nil, rpc.ErrRpcInternal
		}

		if task.Status != v1.TaskStatusPending {
			task = nil
		}
	}

	if task != nil {
		return task, nil
	}

	logger.Info("getting force task list")

	ft, err := s.miners.GetForceTaskList(ctx, &prototypes.Empty{})
	if err != nil {
		logFailedTo(logger, "get force task ids", err)
		return nil, rpc.ErrRpcInternal
	}

	logger.Infof("force task list: %+v", ft)

	logger.Info("getting pending task (default)")

	task, err = s.dm.GetPendingTask(ctx, ft.Ids, nil, false)
	if err != nil {
		logFailedTo(s.logger, "get pending task", err)
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	taskLogFound := false
	taskLog, err := s.dm.GetTaskLog(ctx, task.ID)
	if err == nil {
		for _, taskLogItem := range taskLog {
			if taskLogItem.ID == task.ID {
				taskLogFound = true
			}
		}
	}

	if taskLogFound {
		ft.Ids = append(ft.Ids, task.ID)

		logger.Info("getting pending task (exclude task ids)")

		task, err = s.dm.GetPendingTask(ctx, ft.Ids, nil, false)
		if err != nil {
			logFailedTo(s.logger, "get pending task (retry)", err)
			return nil, rpc.ErrRpcInternal
		}
	}

	if hw, ok := miner.Tags["hw"]; ok {
		logger.Info("raspberrypi pi miner")

		fullHDProfileID := "45d5ef05-efef-4606-6fa3-48f42d3f0b96"
		if hw == "raspberrypi" {
			if task.ProfileID != fullHDProfileID && task.IsOutputFile() {
				cmdline := strings.Replace(task.Cmdline, "-c:v libx264", "-c:v h264_omx", -1)
				err := s.dm.UpdateTaskCommandLine(ctx, task, cmdline)
				if err != nil {
					logFailedTo(s.logger, "update command line for raspberrypi", err)
					return nil, rpc.ErrRpcInternal
				}
			} else {
				excludeProfileIds := []string{fullHDProfileID}
				task, err = s.dm.GetPendingTask(ctx, ft.Ids, excludeProfileIds, true)
				if err != nil {
					logFailedTo(s.logger, "get pending task for raspberrypi", err)
					return nil, rpc.ErrRpcInternal
				}

				if task == nil {
					return nil, rpc.ErrRpcNotFound
				}
			}
		}
	}

	return task, nil
}

func (s *Server) markStreamAsCompletedIfNeeded(task *datastore.Task) {
	ctx := context.Background()

	logger := s.logger.
		WithField("id", task.ID).
		WithField("stream_id", task.StreamID)

	if task.ID != task.StreamID {
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
			logger.Infof("complete stream")

			_, err := s.streams.Complete(context.Background(), &pstreamsv1.StreamRequest{Id: task.StreamID})
			if err != nil {
				logFailedTo(s.logger, "file publish done", err)
				return
			}
		}
	}
}

func (s *Server) markStreamAsFailedIfNeeded(task *datastore.Task) {
	ctx := context.Background()

	logger := s.logger.
		WithField("id", task.ID).
		WithField("stream_id", task.StreamID)

	if task.ID != task.StreamID {
		logger.Info("getting tasks by stream")
		relTasks, err := s.dm.GetTasksByStreamID(ctx, task.StreamID)
		if err != nil {
			logFailedTo(s.logger, "get tasks by stream", err)
			return
		}

		for _, relTask := range relTasks {
			if relTask.ID != task.ID {
				if relTask.Status == v1.TaskStatusAssigned || relTask.Status == v1.TaskStatusPending ||
					relTask.Status == v1.TaskStatusCreated || relTask.Status == v1.TaskStatusEncoding {
					err := s.dm.MarkTaskAsCanceled(ctx, relTask)
					if err != nil {
						logFailedTo(s.logger, "mark task as canceled", err)
						return
					}
				}
			}
		}
	}

	_, err := s.streams.UpdateStatus(
		context.Background(),
		&pstreamsv1.UpdateStatusRequest{ID: task.StreamID, Status: streamsv1.StreamStatusFailed},
	)
	if err != nil {
		logFailedTo(s.logger, "update stream status", err)
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
				logFailedTo(s.logger, "mark task as pending (failed)", err)
			} else {
				err := s.dm.ClearClientID(ctx, task)
				if err != nil {
					s.logger.
						WithField("task_id", task.ID).
						Errorf("failed to clear client id: %s", err)
				}
				isRetryable = true
			}
		}
	}

	return isRetryable
}

func (s *Server) assignTask(task *datastore.Task, miner *minersv1.MinerResponse) error {
	logger := s.logger.WithField("client_id", miner.Id)

	task.ClientID = dbr.NewNullString(miner.Id)

	ctx := context.Background()
	err := s.dm.MarkTaskAsAssigned(ctx, task)
	if err != nil {
		logFailedTo(logger, "mark as assigned", err)
		return rpc.ErrRpcInternal
	}

	logger.WithField("assigned_client_id", task.ClientID.String).Info("task has been assigned")

	atReq := &minersv1.AssignTaskRequest{
		ClientID: task.ClientID.String,
		TaskID:   task.ID,
	}

	_, err = s.miners.AssignTask(ctx, atReq)
	if err != nil {
		logFailedTo(logger, "assign task to miners service", err)
	}

	err = s.dm.LogTask(ctx, miner.Id, task.ID)
	if err != nil {
		logFailedTo(logger, "failed to log task", err)
	}

	return nil
}
