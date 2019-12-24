package v1

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

func (task *Task) GetAngles(video *VideoOutput) []uint32 {
	angles := []uint32{}

	angle := video.Yaw
	delta := 360 / video.TotalStreams
	step := video.TotalStreams / video.Streams

	for {
		angles = append(angles, angle)
		angle += delta * step
		if angle >= 360 {
			break
		}
	}

	return angles
}

func (task *Task) IsLive() bool {
	return task.Output.IsLive()
}

func (task *Task) GetOutputDir(baseDir string) string {
	return fmt.Sprintf("%s/%d/%s", baseDir, task.UserId, task.BroadcastId)
}

func (task *Task) GetOutputFilename(video *VideoOutput, angle uint32) string {
	return fmt.Sprintf("%s-.%s", task.GetSessionID(video, angle), getExtForFormat(video.Format))
}

func (task *Task) GetSessionID(video *VideoOutput, angle uint32) string {
	resolution := fmt.Sprintf("%dx%d", video.Width, video.Height)
	if video.CuboidWidth > 0 && video.CuboidHeight > 0 && video.CuboidTbLength > 0 {
		height := video.CuboidHeight * 2
		if task.Input.Video.StereoFormat == "mono180" {
			height = video.CuboidHeight
		}
		resolution = fmt.Sprintf("%dx%d", video.CuboidWidth*2+video.CuboidTbLength, height)
	}

	return fmt.Sprintf("%s-%s-%dK-%d", task.PlatformId, resolution, video.Bitrate, angle)
}

func (task *Task) GetSegmentObjectName(video *VideoOutput, angle uint32, name string) string {
	sessionID := task.GetSessionID(video, angle)
	return fmt.Sprintf(
		"%d/broadcasts/%s/%s/%s",
		task.UserId,
		task.BroadcastId,
		sessionID,
		name,
	)
}

func (in TaskInput) Value() (driver.Value, error) {
	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	b, err := m.Marshal(in)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (in *TaskInput) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed")
	}

	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	return m.Unmarshal(source, in)
}

func (out TaskOutput) Value() (driver.Value, error) {
	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	b, err := m.Marshal(out)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (out *TaskOutput) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed")
	}

	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	return m.Unmarshal(source, out)
}

func (out *TaskOutput) IsHLS() bool {
	return out.Video[0].Format == "hls"
}

func (out *TaskOutput) IsFLV() bool {
	return out.Video[0].Format == "flv"
}

func (out *TaskOutput) IsMP4() bool {
	return out.Video[0].Format == "mp4"
}

func (out *TaskOutput) IsRTMP() bool {
	return out.IsFLV() && out.Video[0].GetRtmpAddress() != ""
}

func (out *TaskOutput) IsLive() bool {
	return out.Video[0].IsLive
}

func getExtForFormat(format string) string {
	switch format {
	case "hls":
		return "m3u8"
	default:
		return format
	}
}
