package datastore

import (
	"context"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/mailru/dbr"
	"github.com/videocoin/cloud-pkg/dbrutil"
	"github.com/videocoin/cloud-pkg/uuid4"
)

type TasksHistoryDatastore struct {
	conn  *dbr.Connection
	table string
}

func NewTasksHistoryDatastore(conn *dbr.Connection) (*TasksHistoryDatastore, error) {
	return &TasksHistoryDatastore{
		conn:  conn,
		table: "tasks_history",
	}, nil
}

func (ds *TasksHistoryDatastore) Log(ctx context.Context, minerID, taskID string) error {
	tx, ok := dbrutil.DbTxFromContext(ctx)
	if !ok {
		sess := ds.conn.NewSession(nil)
		tx, err := sess.Begin()
		if err != nil {
			return err
		}

		defer func() {
			err = tx.Commit()
			tx.RollbackUnlessCommitted()
		}()
	}

	id, err := uuid4.New()
	if err != nil {
		return err
	}

	th := &TaskHistoryItem{
		ID:        id,
		CreatedAt: pointer.ToTime(time.Now()),
		MinerID:   minerID,
		TaskID:    taskID,
	}

	cols := []string{"id", "miner_id", "task_id", "created_at"}
	_, err = tx.InsertInto(ds.table).Columns(cols...).Record(th).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (ds *TasksHistoryDatastore) GetLogByTaskID(ctx context.Context, taskID string) ([]*TaskHistoryItem, error) {
	tx, ok := dbrutil.DbTxFromContext(ctx)
	if !ok {
		sess := ds.conn.NewSession(nil)
		tx, err := sess.Begin()
		if err != nil {
			return nil, err
		}

		defer func() {
			err = tx.Commit()
			tx.RollbackUnlessCommitted()
		}()
	}

	var items []*TaskHistoryItem
	sb := tx.Select("*").From(ds.table).Where("task_id = ?", taskID)
	_, err := sb.LoadStructs(&items)
	if err != nil {
		return nil, err
	}

	return items, nil
}
