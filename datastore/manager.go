package datastore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/grafov/m3u8"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/mailru/dbr"
	"github.com/sirupsen/logrus"
	clientv1 "github.com/videocoin/cloud-api/client/v1"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	profilesv1 "github.com/videocoin/cloud-api/profiles/v1"
	pstreamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/v1"
	"github.com/videocoin/cloud-pkg/dbrutil"
	"github.com/videocoin/cloud-pkg/hls"
	"github.com/videocoin/cloud-pkg/uuid4"
)

type DataManager struct {
	logger *logrus.Entry
	ds     *Datastore
	sc     *clientv1.ServiceClient
}

func NewDataManager(ctx context.Context, ds *Datastore, sc *clientv1.ServiceClient) (*DataManager, error) {
	return &DataManager{
		logger: ctxlogrus.Extract(ctx).WithField("system", "datamanager"),
		ds:     ds,
		sc:     sc,
	}, nil
}

func (m *DataManager) NewContext(ctx context.Context) (context.Context, *dbr.Session, *dbr.Tx, error) {
	sess := m.ds.conn.NewSession(dbrutil.NewLogrusLogger(m.logger))
	tx, err := sess.Begin()
	if err != nil {
		return ctx, nil, nil, err
	}

	ctx = dbrutil.NewContextWithDbSession(ctx, sess)
	ctx = dbrutil.NewContextWithDbTx(ctx, tx)

	return ctx, sess, tx, err
}

func (m *DataManager) CreateTask(ctx context.Context, task *Task) error {
	logger := m.logger

	logger.Info("creating task")

	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("create task", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.Create(ctx, task)
	if err != nil {
		return failedTo("create task", err)
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) CreateTasksFromStreamResponse(
	ctx context.Context,
	stream *pstreamsv1.StreamResponse,
) ([]*Task, error) {
	logger := m.logger.WithFields(logrus.Fields{
		"stream_id":  stream.ID,
		"input_type": stream.InputType.String(),
	})

	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return nil, failedTo("create task from stream id", err)
	}
	defer tx.RollbackUnlessCommitted()

	tasks := []*Task{}

	getProfileReq := &profilesv1.ProfileRequest{
		ID: stream.ProfileID,
	}

	p, err := m.sc.Profiles.Get(ctx, getProfileReq)
	if err != nil {
		return nil, failedTo("get profile", err)
	}

	// File
	if stream.InputType == streamsv1.InputTypeFile {
		logger.Info("creating tasks from stream")
		logger.WithField("url", stream.InputURL).Info("reading hls playlist")

		pl, plType, err := hls.ParseHLSFromURL(stream.InputURL)
		if err != nil {
			return nil, err
		}

		if plType != m3u8.MEDIA {
			return nil, errors.New("playlist type not supported")
		}

		mediapl := pl.(*m3u8.MediaPlaylist)
		for _, segment := range mediapl.Segments {
			if segment == nil {
				continue
			}
			taskID, _ := uuid4.New()
			urlParts := strings.Split(stream.InputURL, "/")
			baseInputURL := strings.Join(urlParts[:len(urlParts)-1], "/")
			inputURL := fmt.Sprintf("%s/%s", baseInputURL, segment.URI)
			outputPath := fmt.Sprintf("$OUTPUT/%s", stream.ID)

			components := []*profilesv1.Component{}
			for _, component := range p.Components {
				if component.Type == profilesv1.ComponentTypeEncoder {
					components = append(components, component)
				}
			}
			if len(components) > 0 {
				muxer := &profilesv1.Component{
					Type: profilesv1.ComponentTypeMuxer,
					Params: []*profilesv1.Param{
						{Key: "-f", Value: "mpegts"},
					},
				}
				components = append(components, muxer)
			}

			segmentNum := extractNumFromSegmentName(segment.URI)
			newSegmentNum := segmentNum + 1
			newSegmentURI := fmt.Sprintf("%d.ts", newSegmentNum)
			profileReq := &profilesv1.RenderRequest{
				ID:         stream.ProfileID,
				Input:      inputURL,
				Output:     fmt.Sprintf("%s/%s", outputPath, newSegmentURI),
				Components: components,
			}
			renderResp, err := m.sc.Profiles.Render(ctx, profileReq)
			if err != nil {
				return nil, failedTo("render profile", err)
			}

			task := &Task{
				ID:        taskID,
				StreamID:  stream.ID,
				UserID:    dbr.NewNullString(stream.UserID),
				CreatedAt: pointer.ToTime(time.Now()),
				ProfileID: stream.ProfileID,
				Status:    v1.TaskStatusPending,
				Input:     &v1.TaskInput{URI: inputURL},
				Output: &v1.TaskOutput{
					Path:     outputPath,
					Name:     newSegmentURI,
					Num:      newSegmentNum,
					Duration: segment.Duration,
				},
				StreamContractID:      dbr.NewNullInt64(stream.StreamContractID),
				StreamContractAddress: dbr.NewNullString(stream.StreamContractAddress),
				MachineType:           dbr.NewNullString(p.MachineType),
				Cmdline:               renderResp.Render,
				IsLive:                false,
				Capacity:              p.Capacity,
			}

			err = m.ds.Tasks.Create(ctx, task)
			if err != nil {
				return nil, failedTo("create task", err)
			}

			tasks = append(tasks, task)
		}

		err = tx.Commit()
		if err != nil {
			return nil, err
		}

		return tasks, nil
	}

	// RTMP, WebRTC

	logger.Info("creating task from stream")

	task := TaskFromStreamResponse(stream)
	task.MachineType = dbr.NewNullString(p.MachineType)
	task.Status = v1.TaskStatusPending
	task.IsLive = true
	task.Capacity = p.Capacity

	profileReq := &profilesv1.RenderRequest{
		ID:     task.ProfileID,
		Input:  task.Input.GetURI(),
		Output: fmt.Sprintf("%s/%s", task.Output.GetPath(), "index.m3u8"),
	}
	renderResp, err := m.sc.Profiles.Render(ctx, profileReq)
	if err != nil {
		return nil, failedTo("render profile", err)
	}
	task.Cmdline = renderResp.Render

	err = m.ds.Tasks.Create(ctx, task)
	if err != nil {
		return nil, failedTo("create task", err)
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (m *DataManager) GetTaskByID(ctx context.Context, id string) (*Task, error) {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return nil, failedTo("get task by id", err)
	}
	defer tx.RollbackUnlessCommitted()

	task, err := m.ds.Tasks.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (m *DataManager) GetPendingTaskByID(ctx context.Context, id string) (*Task, error) {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return nil, failedTo("get task by id", err)
	}
	defer tx.RollbackUnlessCommitted()

	task, err := m.ds.Tasks.GetPendingByID(ctx, id)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (m *DataManager) DeleteTask(ctx context.Context, task *Task) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("get task by id", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.DeleteByID(ctx, task.ID)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) GetPendingTask(ctx context.Context, excludeIds, excludeProfileIds []string, onlyVOD bool, withCapacity *minersv1.CapacityInfo) (*Task, error) {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return nil, failedTo("get pending task", err)
	}
	defer tx.RollbackUnlessCommitted()

	task, err := m.ds.Tasks.GetPendingTask(ctx, excludeIds, excludeProfileIds, onlyVOD, withCapacity)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return task, nil
}

func (m *DataManager) UnlockTask(ctx context.Context, task *Task) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("unlock task", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.Unlock(ctx, task)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) GetTasks(ctx context.Context) ([]*Task, error) {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return nil, failedTo("get tasks", err)
	}
	defer tx.RollbackUnlessCommitted()

	tasks, err := m.ds.Tasks.GetList(ctx)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (m *DataManager) GetTasksByStreamID(ctx context.Context, streamID string) ([]*Task, error) {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return nil, failedTo("get tasks", err)
	}
	defer tx.RollbackUnlessCommitted()

	tasks, err := m.ds.Tasks.GetListByStreamID(ctx, streamID)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (m *DataManager) MarkTaskAsPending(ctx context.Context, task *Task) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("mark task as pending", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.MarkTaskAsPending(ctx, task)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) MarkTaskAsAssigned(ctx context.Context, task *Task) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("mark task as assigned", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.MarkTaskAsAssigned(ctx, task)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) MarkTaskAsCompleted(ctx context.Context, task *Task) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("mark task as completed", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.MarkTaskAsCompleted(ctx, task)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) MarkTaskAsFailed(ctx context.Context, task *Task) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("mark task as failed", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.MarkTaskAsFailed(ctx, task)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) MarkTaskAsCanceled(ctx context.Context, task *Task) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("mark task as failed", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.MarkTaskAsCanceled(ctx, task)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) UpdateTaskStreamContract(ctx context.Context, task *Task, id int64, address string) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("update task stream contract", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.UpdateStreamContract(ctx, task, id, address)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) UpdateTaskCommandLine(ctx context.Context, task *Task, cmdline string) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("update task command line", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.UpdateCommandLine(ctx, task, cmdline)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) LogTask(ctx context.Context, minerID, taskID string) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("update task stream contract", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.TasksHistory.Log(ctx, minerID, taskID)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) GetTaskLog(ctx context.Context, taskID string) ([]*TaskHistoryItem, error) {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return nil, failedTo("update task stream contract", err)
	}
	defer tx.RollbackUnlessCommitted()

	items, err := m.ds.TasksHistory.GetLogByTaskID(ctx, taskID)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return items, nil
}

func (m *DataManager) ClearClientID(ctx context.Context, task *Task) error {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("clear client id", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.Tasks.ClearClientID(ctx, task)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) GetProfile(ctx context.Context, profileID string) (*profilesv1.GetProfileResponse, error) {
	return m.sc.Profiles.Get(ctx, &profilesv1.ProfileRequest{
		ID: profileID,
	})
}

func (m *DataManager) GetStream(ctx context.Context, streamID string) (*pstreamsv1.StreamResponse, error) {
	return m.sc.Streams.Get(ctx, &pstreamsv1.StreamRequest{Id: streamID})
}

func (m *DataManager) CreateTaskTx(ctx context.Context, taskTx *TaskTx) error {
	logger := m.logger

	logger.Info("creating task tx")

	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("create task", err)
	}
	defer tx.RollbackUnlessCommitted()

	err = m.ds.TaskTxs.Create(ctx, taskTx)
	if err != nil {
		return failedTo("create task tx", err)
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) UpdateProof(ctx context.Context, data UpdateProof) error {
	logger := ctxlogrus.Extract(ctx)

	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("create task tx", err)
	}
	defer tx.RollbackUnlessCommitted()

	logger.Info("getting task tx by stream contract addrees and chunk id")

	taskTx, err := m.ds.TaskTxs.GetByStreamContractAddressAndChunkID(ctx, data.StreamContractAddress, data.ChunkID)
	if err != nil {
		return failedTo("get task tx by stream contract address and chunk id", err)
	}

	if taskTx.SubmitProofTx.String != "" || taskTx.ValidateProofTx.String != "" || taskTx.ScrapProofTx.String != "" {
		return errors.New("proof data already exist")
	}

	logger.Info("updating proof")

	err = m.ds.TaskTxs.UpdateProof(ctx, taskTx, data)
	if err != nil {
		return failedTo("update validate proof", err)
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (m *DataManager) AddInputChunk(ctx context.Context, data AddInputChunk) error {
	logger := ctxlogrus.Extract(ctx)

	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return failedTo("create task tx", err)
	}
	defer tx.RollbackUnlessCommitted()

	logger.Info("getting task by stream id")

	task, err := m.ds.Tasks.GetByID(ctx, data.StreamID)
	if err != nil {
		return failedTo("get task by stream id", err)
	}

	logger = logger.WithField("task_id", task.ID)
	logger.Info("getting task tx by stream contract id and chunk id")

	_, err = m.ds.TaskTxs.GetByStreamContractIDAndChunkID(ctx, data.StreamContractID, data.ChunkID)
	if err != nil {
		if err == ErrTaskTxNotFound {
			newTaskTx := &TaskTx{
				TaskID:                task.ID,
				StreamContractID:      strconv.FormatInt(task.StreamContractID.Int64, 10),
				StreamContractAddress: task.StreamContractAddress.String,
				ChunkID:               data.ChunkID,
				AddInputChunkTx:       dbr.NewNullString(data.AddInputChunkTx),
				AddInputChunkTxStatus: dbr.NewNullString(data.AddInputChunkTxStatus.String()),
			}

			logger.Info("creating task tx")

			createErr := m.CreateTaskTx(ctx, newTaskTx)
			if createErr != nil {
				logger.WithError(err).Error("failed to create tx")
			}
		} else {
			return failedTo("get task tx by stream contract id and chunk id", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
