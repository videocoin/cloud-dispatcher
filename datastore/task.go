package datastore

import (
	"fmt"
	"strings"
	"time"

	"github.com/AlekSi/pointer"
	"github.com/mailru/dbr"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	pstreamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
)

type Task struct {
	ID                    string                 `db:"id"`
	StreamID              string                 `db:"stream_id"`
	OwnerID               int32                  `db:"owner_id"`
	UserID                dbr.NullString         `db:"user_id"`
	CreatedAt             *time.Time             `db:"created_at"`
	Status                v1.TaskStatus          `db:"status"`
	ProfileID             string                 `db:"profile_id"`
	Input                 *v1.TaskInput          `db:"input"`
	Output                *v1.TaskOutput         `db:"output"`
	Cmdline               string                 `db:"cmdline"`
	ClientID              dbr.NullString         `db:"client_id"`
	StreamContractID      dbr.NullInt64          `db:"stream_contract_id"`
	StreamContractAddress dbr.NullString         `db:"stream_contract_address"`
	MachineType           dbr.NullString         `db:"machine_type"`
	IsLive                bool                   `db:"is_live"`
	Capacity              *minersv1.CapacityInfo `db:"capacity"`
}

func TaskFromStreamResponse(s *pstreamsv1.StreamResponse) *Task {
	out := &v1.TaskOutput{Path: fmt.Sprintf("$OUTPUT/%s", s.ID)}

	return &Task{
		ID:        s.ID,
		StreamID:  s.ID,
		UserID:    dbr.NewNullString(s.UserID),
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

func (s *Task) IsOutputHLS() bool {
	return strings.HasSuffix(s.Cmdline, ".m3u8")
}

func (s *Task) IsOutputFile() bool {
	return strings.HasSuffix(s.Cmdline, ".ts")
}
