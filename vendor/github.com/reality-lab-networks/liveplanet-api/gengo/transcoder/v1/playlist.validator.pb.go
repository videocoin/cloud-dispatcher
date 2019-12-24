// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: transcoder/v1/playlist.proto

package v1

import fmt "fmt"
import github_com_mwitkow_go_proto_validators "github.com/mwitkow/go-proto-validators"
import proto "github.com/golang/protobuf/proto"
import math "math"
import _ "github.com/gogo/protobuf/gogoproto"
import _ "github.com/mwitkow/go-proto-validators"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

func (this *Playlist) Validate() error {
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
func (this *PlaylistVariant) Validate() error {
	if !(this.Bitrate > 999) {
		return github_com_mwitkow_go_proto_validators.FieldError("Bitrate", fmt.Errorf(`value '%v' must be greater than '999'`, this.Bitrate))
	}
	if !(this.Bitrate < 50001) {
		return github_com_mwitkow_go_proto_validators.FieldError("Bitrate", fmt.Errorf(`value '%v' must be less than '50001'`, this.Bitrate))
	}
	return nil
}
func (this *PlaylistSegment) Validate() error {
	return nil
}
