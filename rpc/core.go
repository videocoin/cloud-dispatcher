package rpc

import (
	"context"
	"fmt"

	prototypes "github.com/gogo/protobuf/types"
	"github.com/mailru/dbr"
	"github.com/opentracing/opentracing-go"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"go.uber.org/zap"
)

func (s *Server) getPendingTask(ctx context.Context, miner *minersv1.MinerResponse) (*datastore.Task, error) {
	span := opentracing.SpanFromContext(ctx)

	var err error
	var task *datastore.Task

	if forceTaskID, ok := miner.Tags["force_task_id"]; ok {
		span.LogKV("event", "miner has force task id")

		task, err = s.dm.GetPendingTaskByID(ctx, forceTaskID)
		if err != nil {
			fmtErr := fmt.Errorf("failed to get force task: %s", err)
			return nil, rpc.NewRpcInternalError(fmtErr)
		}

		if task.Status != v1.TaskStatusPending {
			task = nil
		}
	}

	if task != nil {
		return task, nil
	}

	span.LogKV("event", "getting force task list")

	ft, err := s.sc.Miners.GetForceTaskList(ctx, &prototypes.Empty{})
	if err != nil {
		fmtErr := fmt.Errorf("failed to get force task ids: %s", err)
		return nil, rpc.NewRpcInternalError(fmtErr)
	}

	span.SetTag("force_task_ids", ft.Ids)
	span.LogKV("event", "getting pending task")

	task, err = s.dm.GetPendingTask(ctx, ft.Ids, nil, false, miner.CapacityInfo)
	if err != nil {
		fmtErr := fmt.Errorf("failed to get pending task: %s", err)
		return nil, rpc.NewRpcInternalError(fmtErr)
	}
	if task == nil {
		return nil, nil
	}

	// taskLogFound := false
	// taskLog, err := s.dm.GetTaskLog(ctx, task.ID)
	// if err == nil {
	// 	for _, taskLogItem := range taskLog {
	// 		if taskLogItem.ID == task.ID {
	// 			taskLogFound = true
	// 		}
	// 	}
	// }

	// if taskLogFound {
	// 	ft.Ids = append(ft.Ids, task.ID)

	// 	// logger.Info("getting pending task (exclude task ids)")

	// 	task, err = s.dm.GetPendingTask(ctx, ft.Ids, nil, false, miner.CapacityInfo)
	// 	if err != nil {
	// 		// logFailedTo(s.logger, "get pending task (retry)", err)
	// 		return nil, rpc.ErrRpcInternal
	// 	}
	// }

	if hw, ok := miner.Tags["hw"]; ok {
		span.SetTag("miner_hw", hw)
		if hw == "raspberrypi" {
			return nil, nil
		}
	}

	// ok, err := s.isMinerQualify(ctx, miner, task)
	// if err != nil {
	// 	// logFailedTo(logger, "qualify miner", err)
	// 	return nil, rpc.ErrRpcInternal
	// }

	// if !ok {
	// 	return nil, rpc.ErrRpcNotFound
	// }

	return task, nil
}

func (s *Server) assignTask(ctx context.Context, task *datastore.Task, miner *minersv1.MinerResponse) error {
	span := opentracing.SpanFromContext(ctx)
	span.LogKV("event", "marking task as assigned")

	task.ClientID = dbr.NewNullString(miner.Id)
	err := s.dm.MarkTaskAsAssigned(ctx, task)
	if err != nil {
		return fmt.Errorf("failed to mark task as assigned: %s", err)
	}

	span.LogKV("event", "assigning task to miner")

	atReq := &minersv1.AssignTaskRequest{
		ClientID: task.ClientID.String,
		TaskID:   task.ID,
	}
	_, err = s.sc.Miners.AssignTask(ctx, atReq)
	if err != nil {
		return fmt.Errorf("failed to assign task to miner: %s", err)
	}

	// err = s.dm.LogTask(ctx, miner.Id, task.ID)
	// if err != nil {
	// 	logFailedTo(logger, "failed to log task", err)
	// }

	return nil
}

func (s *Server) markTaskAsFailed(ctx context.Context, task *datastore.Task) {
	err := s.dm.MarkTaskAsFailed(ctx, task)
	if err != nil {
		s.logger.Error("failed to mark task as failed", zap.Error(err))
		return
	}

	s.markStreamAsFailedIfNeeded(ctx, task)
}
