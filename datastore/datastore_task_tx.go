package datastore

import (
	"context"
	"errors"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/mailru/dbr"
	"github.com/videocoin/cloud-pkg/dbrutil"
	"github.com/videocoin/cloud-pkg/uuid4"
)

var (
	ErrTaskTxNotFound = errors.New("task tx is not found")
)

type TaskTxDatastore struct {
	conn  *dbr.Connection
	table string
}

func NewTaskTxDatastore(conn *dbr.Connection) (*TaskTxDatastore, error) {
	return &TaskTxDatastore{
		conn:  conn,
		table: "tasks_tx",
	}, nil
}

func (ds *TaskTxDatastore) Create(ctx context.Context, taskTx *TaskTx) error {
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

	if taskTx.ID == "" {
		id, err := uuid4.New()
		if err != nil {
			return err
		}

		taskTx.ID = id
	}

	if taskTx.CreatedAt == nil || taskTx.CreatedAt.IsZero() {
		taskTx.CreatedAt = pointer.ToTime(time.Now())
	}

	cols := []string{
		"id", "task_id", "created_at", "stream_contract_id", "stream_contract_address", "chunk_id",
		"add_input_chunk_tx", "add_input_chunk_tx_status",
		"submit_proof_tx", "submit_proof_tx_status",
		"validate_proof_tx", "validate_proof_tx_status",
		"scrap_proof_tx", "scrap_proof_tx_status"}
	_, err := tx.InsertInto(ds.table).Columns(cols...).Record(taskTx).Exec()
	if err != nil {
		return err
	}

	return nil
}

func (ds *TaskTxDatastore) DeleteByID(ctx context.Context, id string) error {
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

func (ds *TaskTxDatastore) GetList(ctx context.Context) ([]*TaskTx, error) {
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

	tasktxs := []*TaskTx{}

	sb := tx.Select("*").From(ds.table)
	_, err := sb.LoadStructs(&tasktxs)
	if err != nil {
		return nil, err
	}

	return tasktxs, nil
}

func (ds *TaskTxDatastore) GetByID(ctx context.Context, id string) (*TaskTx, error) {
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

	taskTx := new(TaskTx)
	_, err := tx.Select("*").From(ds.table).Where("id = ?", id).Load(taskTx)
	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	return taskTx, nil
}

func (ds *TaskTxDatastore) GetByStreamContractAddressAndChunkID(ctx context.Context, sca string, chunkID int64) (*TaskTx, error) {
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

	taskTx := new(TaskTx)
	err := tx.Select("*").
		From(ds.table).
		Where("stream_contract_address = ? AND chunk_id = ?", sca, chunkID).
		LoadStruct(taskTx)
	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, ErrTaskTxNotFound
		}
		return nil, err
	}

	return taskTx, nil
}

func (ds *TaskTxDatastore) GetByStreamContractIDAndChunkID(ctx context.Context, sci string, chunkID int64) (*TaskTx, error) {
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

	taskTx := new(TaskTx)
	err := tx.Select("*").
		From(ds.table).
		Where("stream_contract_id = ? AND chunk_id = ?", sci, chunkID).
		LoadStruct(taskTx)
	if err != nil {
		if err == dbr.ErrNotFound {
			return nil, ErrTaskTxNotFound
		}
		return nil, err
	}

	return taskTx, nil
}

func (ds *TaskTxDatastore) UpdateProof(ctx context.Context, taskTx *TaskTx, data UpdateProof) error {
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

	taskTx.SubmitProofTx = dbr.NewNullString(data.SubmitProofTx)
	taskTx.SubmitProofTxStatus = dbr.NewNullString(data.SubmitProofTxStatus.String())
	taskTx.ValidateProofTx = dbr.NewNullString(data.ValidateProofTx)
	taskTx.ValidateProofTxStatus = dbr.NewNullString(data.ValidateProofTxStatus.String())
	taskTx.ScrapProofTx = dbr.NewNullString(data.ScrapProofTx)
	taskTx.ScrapProofTxStatus = dbr.NewNullString(data.ScrapProofTxStatus.String())

	builder := tx.
		Update(ds.table).
		Where("id = ?", taskTx.ID)

	if taskTx.SubmitProofTx.String != "" {
		builder = builder.
			Set("submit_proof_tx", taskTx.SubmitProofTx).
			Set("submit_proof_tx_status", taskTx.SubmitProofTxStatus)
	}

	if taskTx.ValidateProofTx.String != "" {
		builder = builder.
			Set("validate_proof_tx", taskTx.ValidateProofTx).
			Set("validate_proof_tx_status", taskTx.ValidateProofTxStatus)
	}

	if taskTx.ScrapProofTx.String != "" {
		builder = builder.
			Set("scrap_proof_tx", taskTx.ScrapProofTx).
			Set("scrap_proof_tx_status", taskTx.ScrapProofTxStatus)
	}

	_, err := builder.Exec()
	if err != nil {
		return err
	}

	return nil
}
