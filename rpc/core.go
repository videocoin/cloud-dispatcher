package rpc

import (
	"context"
	"fmt"

	prototypes "github.com/gogo/protobuf/types"
	"github.com/mailru/dbr"
	"github.com/opentracing/opentracing-go"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
)

func (s *Server) getPendingTask(ctx context.Context, miner *minersv1.MinerResponse) (*datastore.Task, error) {
	span := opentracing.SpanFromContext(ctx)

	var err error
	var task *datastore.Task

	logger := s.logger.WithField("miner_id", miner.Id)

	if hw, ok := miner.Tags["hw"]; ok {
		span.SetTag("miner_hw", hw)
		if hw == "raspberrypi" {
			return nil, nil
		}
	}

	if forceTaskID, ok := miner.Tags["force_task_id"]; ok {
		span.LogKV("event", "miner has force task id")

		task, err = s.dm.GetPendingTaskByID(ctx, forceTaskID)
		if err != nil {
			if err != datastore.ErrTaskNotFound {
				logger.WithError(err).Error("failed to get pending task by id")
				return nil, err
			}
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
		logger.WithError(err).Error("failed to get force task list")
		return nil, err
	}

	span.SetTag("force_task_ids", ft.Ids)
	span.SetTag("capacity_info", miner.CapacityInfo)
	span.LogKV("event", "getting pending task")

	task, err = s.dm.GetPendingTask(ctx, ft.Ids, nil, false, miner.CapacityInfo)
	if err != nil {
		if err != datastore.ErrTaskNotFound {
			logger.WithError(err).Error("failed to pending task")
			return nil, err
		}

		return nil, nil
	}

	if task == nil || task.ID == "" {
		return nil, nil
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

	return nil
}

func (s *Server) markTaskAsFailed(ctx context.Context, task *datastore.Task) {
	err := s.dm.MarkTaskAsFailed(ctx, task)
	if err != nil {
		s.logger.WithError(err).Error("failed to mark task as failed")
		return
	}

	s.markStreamAsFailedIfNeeded(ctx, task)
}
