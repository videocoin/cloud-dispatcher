package datastore

import "time"

type TaskHistoryItem struct {
	ID        string     `db:"id"`
	MinerID   string     `db:"miner_id"`
	TaskID    string     `db:"task_id"`
	CreatedAt *time.Time `db:"created_at"`
}
