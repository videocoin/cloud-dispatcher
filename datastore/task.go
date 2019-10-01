package datastore

import (
	"fmt"
	"time"

	"github.com/AlekSi/pointer"
	dispatcherv1 "github.com/videocoin/cloud-api/dispatcher/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
)

type Task struct {
	*dispatcherv1.Task
}

func TaskFromStreamResponse(s *streamsv1.StreamResponse) *Task {
	out := &dispatcherv1.TaskOutput{Path: fmt.Sprintf("$OUTPUT/%s", s.ID)}

	return &Task{
		Task: &dispatcherv1.Task{
			ID:        s.ID,
			OwnerID:   0,
			CreatedAt: pointer.ToTime(time.Now()),
			ProfileID: s.ProfileID,
			Status:    dispatcherv1.TaskStatusCreated,
			Input: &dispatcherv1.TaskInput{
				URI: s.InputURL,
			},
			Output: out,
		},
	}
}
