package datastore

import (
	_ "github.com/go-sql-driver/mysql" // nolint
	"github.com/mailru/dbr"
)

type Datastore struct {
	conn *dbr.Connection

	Tasks   *TaskDatastore
	TaskTxs *TaskTxDatastore
}

func NewDatastore(uri string) (*Datastore, error) {
	ds := new(Datastore)

	conn, err := dbr.Open("mysql", uri, nil)
	if err != nil {
		return nil, err
	}

	err = conn.Ping()
	if err != nil {
		return nil, err
	}

	ds.conn = conn

	tasksDs, err := NewTaskDatastore(conn)
	if err != nil {
		return nil, err
	}

	ds.Tasks = tasksDs

	taskTxsDs, err := NewTaskTxDatastore(conn)
	if err != nil {
		return nil, err
	}

	ds.TaskTxs = taskTxsDs

	return ds, nil
}
