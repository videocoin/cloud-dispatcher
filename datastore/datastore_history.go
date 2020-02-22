package datastore

import (
	"context"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/mailru/dbr"
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
	var sess *dbr.Session
	var tx *dbr.Tx

	sess, _ = DbSessionFromContext(ctx)
	if sess == nil {
		sess = ds.conn.NewSession(nil)
	}

	tx, _ = DbTxFromContext(ctx)
	if tx == nil {
		tx, err := sess.Begin()
		if err != nil {
			return err
		}

		defer func() {
			tx.Commit()
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
	var items []*TaskHistoryItem
	var sess *dbr.Session
	var tx *dbr.Tx

	sess, _ = DbSessionFromContext(ctx)
	if sess == nil {
		sess = ds.conn.NewSession(nil)
	}

	tx, _ = DbTxFromContext(ctx)
	if tx == nil {
		tx, err := sess.Begin()
		if err != nil {
			return nil, err
		}

		defer func() {
			tx.Commit()
			tx.RollbackUnlessCommitted()
		}()
	}

	sb := tx.Select("*").From(ds.table).Where("task_id = ?", taskID)
	_, err := sb.LoadStructs(&items)
	if err != nil {
		return nil, err
	}

	return items, nil
}
