package datastore

import (
	"fmt"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/mailru/dbr"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
)

type Task struct {
	ID        string         `db:"id"`
	OwnerID   int32          `db:"owner_id"`
	CreatedAt *time.Time     `db:"created_at"`
	Status    v1.TaskStatus  `db:"status"`
	ProfileID string         `db:"profile_id"`
	Input     *v1.TaskInput  `db:"input"`
	Output    *v1.TaskOutput `db:"output"`
	Cmdline   string         `db:"cmdline"`
	MachineID dbr.NullString `db:"machine_id"`
}

func TaskFromStreamResponse(s *streamsv1.StreamResponse) *Task {
	out := &v1.TaskOutput{Path: fmt.Sprintf("$OUTPUT/%s", s.ID)}

	return &Task{
		ID:        s.ID,
		OwnerID:   0,
		CreatedAt: pointer.ToTime(time.Now()),
		ProfileID: s.ProfileID,
		Status:    v1.TaskStatusCreated,
		Input: &v1.TaskInput{
			URI: s.InputURL,
		},
		Output: out,
	}
}
