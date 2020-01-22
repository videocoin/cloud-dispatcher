package datastore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/grafov/m3u8"
	"github.com/mailru/dbr"
	"github.com/sirupsen/logrus"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	profilesv1 "github.com/videocoin/cloud-api/profiles/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	streamsv1p "github.com/videocoin/cloud-api/streams/v1"
	"github.com/videocoin/cloud-pkg/hls"
	"github.com/videocoin/cloud-pkg/uuid4"
)

type DataManager struct {
	logger   *logrus.Entry
	ds       *Datastore
	streams  streamsv1.StreamsServiceClient
	profiles profilesv1.ProfilesServiceClient
}

func NewDataManager(
	ds *Datastore,
	streams streamsv1.StreamsServiceClient,
	profiles profilesv1.ProfilesServiceClient,
	logger *logrus.Entry,
) (*DataManager, error) {
	return &DataManager{
		logger:   logger,
		ds:       ds,
		streams:  streams,
		profiles: profiles,
	}, nil
}

func (m *DataManager) NewContext(ctx context.Context) (context.Context, *dbr.Session, *dbr.Tx, error) {
	dbLogger := NewDatastoreLogger(m.logger)
	sess := m.ds.conn.NewSession(dbLogger)
	tx, err := sess.Begin()
	if err != nil {
		return ctx, nil, nil, err
	}

	ctx = NewContextWithDbSession(ctx, sess)
	ctx = NewContextWithDbTx(ctx, tx)

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

func (m *DataManager) CreateTasksFromStreamID(ctx context.Context, streamID string) (*Task, error) {
	logger := m.logger.WithField("stream_id", streamID)

	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return nil, failedTo("create task from stream id", err)
	}
	defer tx.RollbackUnlessCommitted()

	streamReq := &streamsv1.StreamRequest{Id: streamID}
	streamResp, err := m.streams.Get(ctx, streamReq)
	if err != nil {
		return nil, failedTo("get stream", err)
	}

	getProfileReq := &profilesv1.ProfileRequest{
		Id: streamResp.ProfileID,
	}

	p, err := m.profiles.Get(ctx, getProfileReq)
	if err != nil {
		return nil, failedTo("get profile", err)
	}

	logger.Debugf("profile %+v\n", p)

	if streamResp.InputType == streamsv1p.InputTypeFile {
		logger.Info("creating tasks from stream id")
		logger.Infof("reading hls playlist %s", streamResp.InputURL)

		pl, plType, err := hls.ParseHLSFromURL(streamResp.InputURL)
		if err != nil {
			return nil, err
		}

		if plType != m3u8.MEDIA {
			return nil, errors.New("playlist type not supported")
		}

		mediapl := pl.(*m3u8.MediaPlaylist)
		for _, segment := range mediapl.Segments {
			taskID, _ := uuid4.New()
			urlParts := strings.Split(streamResp.InputURL, "/")
			baseInputURL := strings.Join(urlParts[:len(urlParts)-1], "/")
			inputURL := fmt.Sprintf("%s/%s", baseInputURL, segment.URI)
			task := &Task{
				ID:        taskID,
				StreamID:  streamResp.ID,
				OwnerID:   0,
				CreatedAt: pointer.ToTime(time.Now()),
				ProfileID: streamResp.ProfileID,
				Status:    v1.TaskStatusCreated,
				Input: &v1.TaskInput{
					URI: inputURL,
				},
				Output:                &v1.TaskOutput{Path: fmt.Sprintf("$OUTPUT/%s", taskID)},
				StreamContractID:      dbr.NewNullInt64(streamResp.StreamContractID),
				StreamContractAddress: dbr.NewNullString(streamResp.StreamContractAddress),
			}

			fmt.Printf("task %+v\n", task)
		}

		return nil, nil
	}

	logger.Info("creating task from stream id")

	task := TaskFromStreamResponse(streamResp)

	logger.Debugf("task %+v\n", task)

	task.MachineType = dbr.NewNullString(p.MachineType)

	profileReq := &profilesv1.RenderRequest{
		Id:     task.ProfileID,
		Input:  task.Input.GetURI(),
		Output: task.Output.GetPath(),
	}
	renderResp, err := m.profiles.Render(ctx, profileReq)
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

	return task, nil
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

func (m *DataManager) GetPendingTask(ctx context.Context, excludeIds []string) (*Task, error) {
	ctx, _, tx, err := m.NewContext(ctx)
	if err != nil {
		return nil, failedTo("get pending task", err)
	}
	defer tx.RollbackUnlessCommitted()

	task, err := m.ds.Tasks.GetPendingTask(ctx, excludeIds)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return task, nil
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
