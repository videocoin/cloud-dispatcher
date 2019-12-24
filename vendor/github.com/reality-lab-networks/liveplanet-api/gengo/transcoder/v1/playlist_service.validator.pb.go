// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: transcoder/v1/playlist_service.proto

package v1

import regexp "regexp"
import fmt "fmt"
import github_com_mwitkow_go_proto_validators "github.com/mwitkow/go-proto-validators"
import proto "github.com/golang/protobuf/proto"
import math "math"
import _ "github.com/golang/protobuf/ptypes/empty"
import _ "google.golang.org/genproto/googleapis/api/annotations"
import _ "github.com/gogo/protobuf/gogoproto"
import _ "github.com/mwitkow/go-proto-validators"
import _ "github.com/reality-lab-networks/liveplanet-api/gengo/rpc"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

var _regex_CreatePlaylistRequest_Kind = regexp.MustCompile("viewported|nonviewported|smoothstep|preview|rtmp")
var _regex_CreatePlaylistRequest_ProjectionType = regexp.MustCompile("offset_cubemap|cuboidmap_31_180")
var _regex_CreatePlaylistRequest_StereoFormat = regexp.MustCompile("mono|tb|lr|mono180|tb180")

func (this *CreatePlaylistRequest) Validate() error {
	if !_regex_CreatePlaylistRequest_Kind.MatchString(this.Kind) {
		return github_com_mwitkow_go_proto_validators.FieldError("Kind", fmt.Errorf(`value '%v' must be a string conforming to regex "viewported|nonviewported|smoothstep|preview|rtmp"`, this.Kind))
	}
	if !_regex_CreatePlaylistRequest_ProjectionType.MatchString(this.ProjectionType) {
		return github_com_mwitkow_go_proto_validators.FieldError("ProjectionType", fmt.Errorf(`value '%v' must be a string conforming to regex "offset_cubemap|cuboidmap_31_180"`, this.ProjectionType))
	}
	if !_regex_CreatePlaylistRequest_StereoFormat.MatchString(this.StereoFormat) {
		return github_com_mwitkow_go_proto_validators.FieldError("StereoFormat", fmt.Errorf(`value '%v' must be a string conforming to regex "mono|tb|lr|mono180|tb180"`, this.StereoFormat))
	}
	for _, item := range this.Variants {
		if item != nil {
			if err := github_com_mwitkow_go_proto_validators.CallValidatorIfExists(item); err != nil {
				return github_com_mwitkow_go_proto_validators.FieldError("Variants", err)
			}
		}
	}
	if nil == this.Metadata {
		return github_com_mwitkow_go_proto_validators.FieldError("Metadata", fmt.Errorf("message must exist"))
	}
	if this.Metadata != nil {
		if err := github_com_mwitkow_go_proto_validators.CallValidatorIfExists(this.Metadata); err != nil {
			return github_com_mwitkow_go_proto_validators.FieldError("Metadata", err)
		}
	}
	return nil
}
func (this *PlaylistResponse) Validate() error {
	for _, item := range this.Variants {
		if item != nil {
			if err := github_com_mwitkow_go_proto_validators.CallValidatorIfExists(item); err != nil {
				return github_com_mwitkow_go_proto_validators.FieldError("Variants", err)
			}
		}
	}
	if this.Metadata != nil {
		if err := github_com_mwitkow_go_proto_validators.CallValidatorIfExists(this.Metadata); err != nil {
			return github_com_mwitkow_go_proto_validators.FieldError("Metadata", err)
		}
	}
	return nil
}
func (this *PlaylistRequest) Validate() error {
	return nil
}
func (this *PlaylistManifest) Validate() error {
	for _, item := range this.Qualities {
		if item != nil {
			if err := github_com_mwitkow_go_proto_validators.CallValidatorIfExists(item); err != nil {
				return github_com_mwitkow_go_proto_validators.FieldError("Qualities", err)
			}
		}
	}
	return nil
}
func (this *PlaylistManifestQuality) Validate() error {
	return nil
}

var _regex_AddSegmentRequest_VariantId = regexp.MustCompile(".")
var _regex_AddSegmentRequest_Name = regexp.MustCompile(".")

func (this *AddSegmentRequest) Validate() error {
	if !_regex_AddSegmentRequest_VariantId.MatchString(this.VariantId) {
		return github_com_mwitkow_go_proto_validators.FieldError("VariantId", fmt.Errorf(`value '%v' must be a string conforming to regex "."`, this.VariantId))
	}
	if !(this.Num > 0) {
		return github_com_mwitkow_go_proto_validators.FieldError("Num", fmt.Errorf(`value '%v' must be greater than '0'`, this.Num))
	}
	if !_regex_AddSegmentRequest_Name.MatchString(this.Name) {
		return github_com_mwitkow_go_proto_validators.FieldError("Name", fmt.Errorf(`value '%v' must be a string conforming to regex "."`, this.Name))
	}
	if !(this.Duration > 0) {
		return github_com_mwitkow_go_proto_validators.FieldError("Duration", fmt.Errorf(`value '%v' must be strictly greater than '0'`, this.Duration))
	}
	if !(this.Duration < 11) {
		return github_com_mwitkow_go_proto_validators.FieldError("Duration", fmt.Errorf(`value '%v' must be strictly lower than '11'`, this.Duration))
	}
	return nil
}
