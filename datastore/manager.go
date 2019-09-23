package datastore

import (
	"context"

	"github.com/mailru/dbr"
	"github.com/sirupsen/logrus"
)

type DataManager struct {
	logger *logrus.Entry
	ds     *Datastore
}

func NewDataManager(ds *Datastore, logger *logrus.Entry) (*DataManager, error) {
	return &DataManager{
		logger: logger,
		ds:     ds,
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
