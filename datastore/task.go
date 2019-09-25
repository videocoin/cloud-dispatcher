package datastore

import (
	"time"

	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
)

type Task struct {
	ID        string     `db:"id"`
	CreatedAt time.Time  `db:"created_at"`
	DeletedAt *time.Time `db:"deleted_at"`
	ProfileID int32      `db:"profile_id"`
}

func TaskFromStreamResponse(s *streamsv1.StreamResponse) *Task {
	return &Task{
		ID:        s.ID,
		ProfileID: int32(s.ProfileID),
	}
}
