package datastore

import "time"

type Task struct {
	ID        string     `db:"id"`
	CreatedAt time.Time  `db:"created_at"`
	DeletedAt *time.Time `db:"deleted_at"`
}
