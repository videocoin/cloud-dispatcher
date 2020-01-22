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
	ID                    string         `db:"id"`
	StreamID              string         `db:"stream_id"`
	OwnerID               int32          `db:"owner_id"`
	CreatedAt             *time.Time     `db:"created_at"`
	Status                v1.TaskStatus  `db:"status"`
	ProfileID             string         `db:"profile_id"`
	Input                 *v1.TaskInput  `db:"input"`
	Output                *v1.TaskOutput `db:"output"`
	Cmdline               string         `db:"cmdline"`
	ClientID              dbr.NullString `db:"client_id"`
	StreamContractID      dbr.NullInt64  `db:"stream_contract_id"`
	StreamContractAddress dbr.NullString `db:"stream_contract_address"`
	MachineType           dbr.NullString `db:"machine_type"`
}

func TaskFromStreamResponse(s *streamsv1.StreamResponse) *Task {
	out := &v1.TaskOutput{Path: fmt.Sprintf("$OUTPUT/%s", s.ID)}

	return &Task{
		ID:        s.ID,
		StreamID:  s.ID,
		OwnerID:   0,
		CreatedAt: pointer.ToTime(time.Now()),
		ProfileID: s.ProfileID,
		Status:    v1.TaskStatusCreated,
		Input: &v1.TaskInput{
			URI: s.InputURL,
		},
		Output:                out,
		StreamContractID:      dbr.NewNullInt64(s.StreamContractID),
		StreamContractAddress: dbr.NewNullString(s.StreamContractAddress),
	}
}
