package v1

import (
	"fmt"
)

func (p *Playlist) GetVariantIds(platformID string) []string {
	variantIds := []string{}
	for _, v := range p.Variants {
		for angle := 0; angle < 360; angle += int(v.Yaw) {
			variantIds = append(variantIds, v.VariantID(platformID, angle))
			if v.Yaw == 0 {
				break
			}
		}
	}
	return variantIds
}

func (v *PlaylistVariant) Resolution() string {
	if v.CuboidWidth > 0 && v.CuboidHeight > 0 && v.CuboidTbLength > 0 {
		height := v.CuboidHeight * 2
		if v.StereoFormat == "mono180" {
			height = v.CuboidHeight
		}
		return fmt.Sprintf("%dx%d", v.CuboidWidth*2+v.CuboidTbLength, height)
	}
	return fmt.Sprintf("%dx%d", v.Width, v.Height)
}

func (v *PlaylistVariant) VariantID(platformID string, angle int) string {
	return fmt.Sprintf("%s-%s-%dK-%d", platformID, v.Resolution(), v.Bitrate, angle)
}

func (m *Metadata) ObjectName() string {
	return fmt.Sprintf("%d/broadcasts/%s/%s.m3u8", m.UserId, m.BroadcastId, m.PlatformId)
}
