package v1

import (
	"database/sql/driver"
	"errors"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

func (spec JobSpec) Value() (driver.Value, error) {
	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	b, err := m.Marshal(spec)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (spec *JobSpec) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed.")
	}

	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	return m.Unmarshal(source, spec)
}

func (job *CreateJobRequest) SetDefaultValues() {
	// job.Status = StatusPending

	video := job.Spec.Output.Video

	if video.Format == "" {
		video.Format = "hls"
	}

	if video.StereoFormat == "" {
		video.StereoFormat = "tb"
	}

	if video.Projection == "" {
		if job.Kind == "viewported" {
			video.Projection = "offset_cubemap"
		} else {
			video.Projection = "equirect"
		}
	}

	if video.Framerate == 0 {
		video.Framerate = job.Spec.Input.Framerate
	}

	if video.Framerate == 0 {
		video.Framerate = 30
	}

	if video.SegmentTime == 0 {
		video.SegmentTime = 1
	}

	for _, group := range job.Spec.Output.Video.Sessions {
		for _, session := range group.Session {

			session.Framerate = video.Framerate
			session.SegmentTime = video.SegmentTime
			session.Format = video.Format
			session.StereoFormat = video.StereoFormat
			session.Projection = video.Projection

			session.IsLive = video.IsLive
			session.RtmpAddress = video.RtmpAddress
			session.BackupRtmpAddress = video.BackupRtmpAddress
			session.GopSize = video.GopSize
			session.IsSmoothstep = job.Spec.Input.IsSmoothProj

			if session.GopSize == 0 {
				session.GopSize = session.Framerate * session.SegmentTime
				if session.GopSize > 250 {
					session.GopSize = 250
				}
			}

			if session.Preset == "" {
				session.Preset = "medium"
			}

			if session.Profile == "" {
				session.Profile = "high"
			}

			if session.Codec == "" {
				session.Codec = "libx264"
			}

			if session.CubemapFaceOrder == "" {
				session.CubemapFaceOrder = "planar"
			}

			if session.ExpandCoef == 0 {
				session.ExpandCoef = 1.01
			}

			if session.Padding == 0 {
				session.Padding = 0.001
			}

			if session.Width == 0 {
				session.Width = session.CubeEdgeLength * 3
			}

			if session.Height == 0 {
				if session.StereoFormat == "tb" {
					session.Height = session.CubeEdgeLength * 4
				}
				if session.StereoFormat == "mono" {
					session.Height = session.CubeEdgeLength * 2
				}
			}

			// // clip resolutions to maximum possible on phones
			// if session.Height > session.Width {
			// 	session.Height = min(session.Height, 3840)
			// 	session.Width = min(session.Width, 2160)
			// } else {
			// 	session.Width = min(session.Width, 3840)
			// 	session.Height = min(session.Height, 2160)
			// }

			if session.TotalStreams == 0 {
				session.TotalStreams = 1
			}

			if session.Streams == 0 {
				session.Streams = session.TotalStreams
			}

			if session.Interpolation == "" {
				session.Interpolation = "cublin"
			}
		}
	}
}

func min(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}
