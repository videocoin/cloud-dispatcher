package datastore

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/mailru/dbr"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	"github.com/videocoin/cloud-pkg/dbrutil"
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

	cols := []string{
		"id", "stream_id", "owner_id", "user_id", "created_at", "status", "profile_id", "input", "output", "cmdline",
		"machine_type", "stream_contract_id", "stream_contract_address", "capacity", "is_live", "is_lock"}

	insertCount := 0
	var insertErr error

	for {
		if insertCount == 3 {
			return insertErr
		}
		insertCount++

		_, err := tx.InsertInto(ds.table).Columns(cols...).Record(task).Exec()
		if err != nil {
			insertErr = err
			if strings.HasPrefix(err.Error(), "Error 1213") {
				continue
			}
			return err
		}

		break
	}

	return nil
}

func (ds *TaskDatastore) DeleteByID(ctx context.Context, id string) error {
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

func (ds *TaskDatastore) GetList(ctx context.Context) ([]*Task, error) {
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

	tasks := []*Task{}

	sb := tx.Select("*").From(ds.table)
	_, err := sb.LoadStructs(&tasks)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (ds *TaskDatastore) GetListByStreamID(ctx context.Context, streamID string) ([]*Task, error) {
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

	tasks := []*Task{}

	sb := tx.Select("*").From(ds.table).Where("stream_id = ?", streamID)
	_, err := sb.LoadStructs(&tasks)
	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (ds *TaskDatastore) GetByID(ctx context.Context, id string) (*Task, error) {
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

	task := new(Task)
	err := tx.Select("*").From(ds.table).Where("id = ?", id).LoadStruct(task)
	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}

	return task, nil
}

func (ds *TaskDatastore) GetPendingByID(ctx context.Context, id string) (*Task, error) {
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

	task := new(Task)
	err := tx.
		Select("*").
		From(ds.table).
		Where("id = ? AND status = ? AND is_lock = ?", id, v1.TaskStatusPending.String(), false).
		ForUpdate().
		Limit(1).
		LoadStruct(task)
	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}

	_, err = tx.Update(ds.table).Where("id = ?", task.ID).Set("is_lock", true).Exec()
	if err != nil {
		return nil, err
	}

	task.IsLock = true

	return task, nil
}

func (ds *TaskDatastore) GetPendingTask(
	ctx context.Context,
	excludeIds, excludeProfileIds []string,
	onlyVOD bool,
	withCapacity *minersv1.CapacityInfo,
) (*Task, error) {
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

	task := &Task{}
	qs := tx.
		Select("*", "JSON_EXTRACT(output, \"$.num\") AS num").
		From(ds.table).
		Where(
			"status IN ? AND client_id IS NULL AND is_lock = ?",
			[]string{v1.TaskStatusPending.String(), v1.TaskStatusPaused.String()},
			false,
		)

	if len(excludeIds) > 0 {
		qs = qs.Where("id NOT IN ?", excludeIds)
	}

	if len(excludeProfileIds) > 0 {
		qs = qs.Where("profile_id NOT IN ?", excludeProfileIds)
	}

	if onlyVOD {
		qs = qs.Where("is_live = 0")
	}

	if withCapacity != nil {
		qs = qs.Where(
			"JSON_EXTRACT(capacity, \"$.encode\") <= ? AND JSON_EXTRACT(capacity, \"$.cpu\") <= ? ",
			withCapacity.Encode,
			withCapacity.Cpu,
		)
	}

	err := qs.
		OrderDir("created_at", true).
		OrderDir("num", true).
		Limit(1).
		ForUpdate().
		LoadStruct(task)

	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, ErrTaskNotFound
		}
		return nil, err
	}

	task.IsLock = true

	_, err = tx.Update(ds.table).Where("id = ?", task.ID).Set("is_lock", task.IsLock).Exec()
	if err != nil {
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

func (ds *TaskDatastore) MarkTaskAsCanceled(ctx context.Context, task *Task) error {
	err := ds.markTaskStatusAs(ctx, task, v1.TaskStatusCanceled)
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskDatastore) MarkTaskAsPaused(ctx context.Context, task *Task) error {
	err := ds.markTaskStatusAs(ctx, task, v1.TaskStatusPaused)
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

	task.Status = status
	builder := tx.
		Update(ds.table).
		Where("id = ?", task.ID).
		Set("status", status)

	if status == v1.TaskStatusAssigned {
		builder = builder.
			Where("status IN ?", []string{v1.TaskStatusPending.String(), v1.TaskStatusPaused.String()}).
			Set("client_id", task.ClientID)
	}

	if status == v1.TaskStatusPaused {
		builder = builder.Set("client_id", dbr.NewNullString(nil))
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

func (ds *TaskDatastore) UpdateCommandLine(ctx context.Context, task *Task, cmdline string) error {
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

	task.Cmdline = cmdline
	builder := tx.
		Update(ds.table).
		Where("id = ?", task.ID).
		Set("cmdline", task.Cmdline)

	_, err := builder.Exec()
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskDatastore) ClearClientID(ctx context.Context, task *Task) error {
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

	task.ClientID = dbr.NewNullString(nil)
	builder := tx.
		Update(ds.table).
		Where("id = ?", task.ID).
		Set("client_id", task.ClientID)

	_, err := builder.Exec()
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskDatastore) Unlock(ctx context.Context, task *Task) error {
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

	task.IsLock = false
	builder := tx.
		Update(ds.table).
		Where("id = ?", task.ID).
		Set("is_lock", task.IsLock)

	_, err := builder.Exec()
	if err != nil {
		return err
	}

	return nil
}
