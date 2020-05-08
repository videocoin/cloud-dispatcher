package rpc

import (
	"github.com/jinzhu/copier"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
)

func toTaskResponse(task *datastore.Task, taskTx *datastore.TaskTx) *v1.Task {
	v1Task := &v1.Task{}

	_ = copier.Copy(v1Task, task)

	v1Task.ClientID = task.ClientID.String
	v1Task.StreamID = task.StreamID
	v1Task.MachineType = task.MachineType.String
	v1Task.StreamContractID = uint64(task.StreamContractID.Int64)
	v1Task.StreamContractAddress = task.StreamContractAddress.String
	v1Task.MachineType = task.MachineType.String
	v1Task.HasSubmitProof = false
	v1Task.HasValidateProof = false
	v1Task.HasScrapProof = false

	if taskTx != nil {
		if taskTx.SubmitProofTx.String != "" {
			v1Task.HasSubmitProof = true
		}
		if taskTx.ValidateProofTx.String != "" {
			v1Task.HasValidateProof = true
		}
		if taskTx.ScrapProofTx.String != "" {
			v1Task.HasScrapProof = true
		}
	}

	return v1Task
}
