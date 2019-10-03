package rpc

import (
	"context"

	"github.com/mailru/dbr"

	"github.com/jinzhu/copier"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	"github.com/videocoin/cloud-api/rpc"
)

func (s *RpcServer) GetTask(ctx context.Context, req *v1.TaskRequest) (*v1.Task, error) {
	task, err := s.dm.GetPendingTask(ctx)
	if err != nil {
		logFailedTo(s.logger, "get task", err)
		return nil, rpc.ErrRpcInternal
	}

	if task == nil {
		return nil, rpc.ErrRpcNotFound
	}

	task.MachineID = dbr.NewNullString(req.MachineID)
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

	v1Task.MachineID = task.MachineID.String

	return v1Task, nil
}
