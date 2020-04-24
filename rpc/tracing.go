package rpc

import (
	"github.com/opentracing/opentracing-go"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
)

func spanErr(span opentracing.Span, err error, event string) {
	if span == nil {
		return
	}

	span.SetTag("error", true)
	span.LogKV("event", event, "message", err)
}

func minerResponseToSpan(span opentracing.Span, miner *minersv1.MinerResponse) {
	if span == nil {
		return
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
}

func taskToSpan(span opentracing.Span, task *datastore.Task) {
	if span == nil {
		return
	}

	span.SetTag("task_id", task.ID)
	if task.Input != nil {
		span.SetTag("input", task.Input.URI)
	}
	span.SetTag("task_created_at", task.CreatedAt)
	span.SetTag("task_cmdline", task.Cmdline)
	span.SetTag("task_user_id", task.UserID.String)
	span.SetTag("task_stream_id", task.StreamID)
	span.SetTag("task_stream_contract_id", task.StreamContractID.Int64)
	span.SetTag("task_stream_contract_address", task.StreamContractAddress.String)
	span.SetTag("task_chunk_num", task.Output.Num)
	span.SetTag("task_profile_id", task.ProfileID)

	if task.IsOutputFile() {
		span.SetTag("vod", true)
	} else {
		span.SetTag("live", true)
	}
}
