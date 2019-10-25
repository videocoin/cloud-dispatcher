package datastore

import (
	"context"
	"errors"
	"time"

	v1 "github.com/videocoin/cloud-api/dispatcher/v1"

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

func (ds *TaskDatastore) GetPendingTask(ctx context.Context) (*Task, error) {
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

	task := &Task{}
	err := tx.
		Select("*").
		From(ds.table).
		Where("status = ?", v1.TaskStatus_name[int32(v1.TaskStatusPending)]).
		Where("client_id IS NULL").
		Limit(1).
		LoadStruct(task)

	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	return task, nil
}

func (ds *TaskDatastore) MarkTaskAsPending(ctx context.Context, task *Task) error {
	err := ds.markTaskStatusAs(ctx, task, v1.TaskStatusPending)
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskDatastore) MarkTaskAsAssigned(ctx context.Context, task *Task) error {
	err := ds.markTaskStatusAs(ctx, task, v1.TaskStatusAssigned)
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskDatastore) MarkTaskAsCompleted(ctx context.Context, task *Task) error {
	err := ds.markTaskStatusAs(ctx, task, v1.TaskStatusCompleted)
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskDatastore) MarkTaskAsFailed(ctx context.Context, task *Task) error {
	err := ds.markTaskStatusAs(ctx, task, v1.TaskStatusFailed)
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskDatastore) markTaskStatusAs(
	ctx context.Context,
	task *Task,
	status v1.TaskStatus,
) error {
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

	task.Status = status
	builder := tx.
		Update(ds.table).
		Where("id = ?", task.ID).
		Set("status", status)

	if status == v1.TaskStatusAssigned {
		builder = builder.
			Where("status = ?", v1.TaskStatusPending).
			Set("client_id", task.ClientID)
	}

	r, err := builder.Exec()
	if err != nil {
		return err
	}

	_, err = r.RowsAffected()
	if err != nil {
		return err
	}

	// if n == 0 {
	// 	return fmt.Errorf("mark status as %s: no rows affected", status)
	// }

	// if n > 1 {
	// 	return fmt.Errorf("mark status as %s: rows affected are %d", status, n)
	// }

	return nil
}

func (ds *TaskDatastore) UpdateStreamContract(
	ctx context.Context,
	task *Task,
	id int64,
	address string,
) error {
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

	task.StreamContractID = dbr.NewNullInt64(id)
	task.StreamContractAddress = dbr.NewNullString(address)
	builder := tx.
		Update(ds.table).
		Where("id = ?", task.ID).
		Set("stream_contract_id", task.StreamContractID).
		Set("stream_contract_address", task.StreamContractAddress)

	_, err := builder.Exec()
	if err != nil {
		return err
	}

	return nil
}
