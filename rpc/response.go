package rpc

import (
	"github.com/jinzhu/copier"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
)

func toTaskResponse(task *datastore.Task) *v1.Task {
	v1Task := &v1.Task{}

	copier.Copy(v1Task, task)

	v1Task.ClientID = task.ClientID.String
	v1Task.StreamID = task.StreamID
	v1Task.MachineType = task.MachineType.String
	v1Task.StreamContractID = uint64(task.StreamContractID.Int64)
	v1Task.StreamContractAddress = task.StreamContractAddress.String
	v1Task.MachineType = task.MachineType.String

	return v1Task
}
