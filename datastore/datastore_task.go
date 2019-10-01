package datastore

import (
	"context"
	"errors"
	"time"

	"github.com/AlekSi/pointer"

	"github.com/mailru/dbr"
	"github.com/videocoin/cloud-pkg/uuid4"
)

var (
	ErrTaskNotFound = errors.New("task is not found")
)

type TaskDatastore struct {
	conn  *dbr.Connection
	table string
}

func NewTaskDatastore(conn *dbr.Connection) (*TaskDatastore, error) {
	return &TaskDatastore{
		conn:  conn,
		table: "tasks",
	}, nil
}

func (ds *TaskDatastore) Create(ctx context.Context, task *Task) error {
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

	if task.ID == "" {
		id, err := uuid4.New()
		if err != nil {
			return err
		}

		task.ID = id
	}

	if task.CreatedAt.IsZero() {
		task.CreatedAt = pointer.ToTime(time.Now())
	}

	cols := []string{"id", "owner_id", "created_at", "status", "profile_id", "input", "output", "cmdline"}
	_, err := tx.InsertInto(ds.table).Columns(cols...).Record(task).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskDatastore) DeleteByID(ctx context.Context, id string) error {
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

	now := time.Now().Format("2006-01-02 15:04:05")
	_, err := tx.
		Update(ds.table).
		Where("id = ?", id).
		Set("deleted_at", now).
		Exec()
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskDatastore) GetByID(ctx context.Context, id string) (*Task, error) {
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

	task := new(Task)
	_, err := tx.Select("*").From(ds.table).Where("id = ?", id).Load(task)
	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	return task, nil
}
