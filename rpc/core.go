package rpc

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	prototypes "github.com/gogo/protobuf/types"
	"github.com/mailru/dbr"
	"github.com/opentracing/opentracing-go"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-api/rpc"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"golang.org/x/mod/semver"
)

func (s *Server) getPendingTask(ctx context.Context, req *v1.TaskPendingRequest, miner *minersv1.MinerResponse) (*datastore.Task, error) {
	span := opentracing.SpanFromContext(ctx)

	var err error
	var task *datastore.Task

	logger := s.logger.WithField("miner_id", miner.Id)

	if miner.IsBlock {
		logger.Warning("miner is blocked")
		return nil, nil
	}

	if s.mode != nil {
		if s.mode.OnlyInternal && !miner.IsInternal {
			logger.Warning("miner is not internal")
			return nil, nil
		}

		if s.mode.MinimalVersion != "" {
			workerVersion := strings.Split(req.Version, "-")[0]
			if semver.IsValid(s.mode.MinimalVersion) {
				if !semver.IsValid(workerVersion) {
					logger.Warning("no valid miner version")
					return nil, nil
				}

				if semver.Compare(s.mode.MinimalVersion, workerVersion) > 0 {
					logger.Warning("miner version is deprecated")
					return nil, nil
				}
			} else {
				logger.WithField("version", s.mode.MinimalVersion).Error("minimal version is not valid")
			}
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

	if miner.IsInternal {
		if task.IsOutputHLS() {
			if req.Type == v1.TaskTypeLive {
				return task, nil
			}
			return task, errors.New("don't allow assign live task to vod worker")
		} else if task.IsOutputFile() {
			if req.Type == v1.TaskTypeVOD {
				logger.Info("checking hw")

				if hw, ok := miner.Tags["hw"]; ok {
					span.SetTag("miner_hw", hw)
					if hw == "raspberrypi" {
						cmdline := strings.Replace(task.Cmdline, "-c:v libx264", "-c:v h264_omx", -1)
						err := s.dm.UpdateTaskCommandLine(ctx, task, cmdline)
						if err != nil {
							logger.WithError(err).Error("update command line for raspberry pi")
							return task, rpc.ErrRpcInternal
						}
					}
				}

				return task, nil
			}
			return task, errors.New("don't allow assign vod task to live worker")
		}
	} else {
		if task.IsOutputHLS() {
			return task, errors.New("don't allow assign live task to external worker")
		}
	}

	logger.Info("checking miner is qualify")

	ok, err := s.isMinerQualify(ctx, miner, task)
	if err != nil {
		logger.WithError(err).Error("failed to qualify miner")
		return task, rpc.ErrRpcInternal
	}

	if !ok {
		return task, rpc.ErrRpcNotFound
	}

	logger.Info("checking hw")

	if hw, ok := miner.Tags["hw"]; ok {
		span.SetTag("miner_hw", hw)
		if hw == "raspberrypi" {
			if task.IsOutputFile() {
				cmdline := strings.Replace(task.Cmdline, "-c:v libx264", "-c:v h264_omx", -1)
				err := s.dm.UpdateTaskCommandLine(ctx, task, cmdline)
				if err != nil {
					logger.WithError(err).Error("update command line for raspberry pi")
					return task, rpc.ErrRpcInternal
				}
			} else {
				return task, rpc.ErrRpcNotFound
			}
		}

		if hw == "jetson" {
			if task.IsOutputFile() {
				cmdline := strings.Replace(task.Cmdline, "-c:v libx264", "-c:v h264_nvmpi", -1)
				err := s.dm.UpdateTaskCommandLine(ctx, task, cmdline)
				if err != nil {
					logger.WithError(err).Error("update command line for jetson")
					return task, rpc.ErrRpcInternal
				}
			} else {
				return task, rpc.ErrRpcNotFound
			}
		}
	}

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
		ClientID: miner.Id,
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

func (s *Server) isMinerQualify(ctx context.Context, miner *minersv1.MinerResponse, task *datastore.Task) (bool, error) {
	resp, err := s.sc.Miners.GetMinersCandidates(ctx, &minersv1.MinersCandidatesRequest{
		EncodeCapacity: task.Capacity.Encode,
		CpuCapacity:    task.Capacity.Cpu,
	})
	if err != nil {
		return false, err
	}

	candidates := []*minersv1.MinerCandidateResponse{}
	for _, item := range resp.Items {
		if !item.IsInternal {
			candidates = append(candidates, item)
		}
	}

	if len(candidates) == 0 {
		return true, nil
	}

	if miner.IsInternal {
		return false, nil
	}

	sort.Slice(candidates[:], func(i, j int) bool {
		return candidates[i].Stake > candidates[j].Stake
	})

	var qStake float64
	for _, m := range candidates {
		qStake += m.Stake
	}

	rand.Seed(time.Now().UnixNano())
	r := rand.Float64()

	var choosenMinerID string
	for _, candidate := range candidates {
		weight := candidate.Stake / qStake
		if weight > r {
			choosenMinerID = candidate.ID
			break
		} else {
			r = r - weight
		}
	}

	if choosenMinerID != miner.Id {
		return false, nil
	}

	return true, nil
}
