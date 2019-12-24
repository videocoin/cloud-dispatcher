package v1

import (
	"fmt"
)

func (m *Metadata) MasterObjectName() string {
	return fmt.Sprintf("%d/broadcasts/%s/%s.m3u8", m.UserId, m.BroadcastId, m.PlatformId)
}

func (m *Metadata) MediaObjectName() string {
	return fmt.Sprintf("%d/broadcasts/%s/%s/%s.m3u8", m.UserId, m.BroadcastId, m.VariantId, m.VariantId)
}

func (m *Metadata) MediaObjectNameWithThreshold(threshold uint64) string {
	return fmt.Sprintf("%d/broadcasts/%s/%s/%s.m3u8_%d", m.UserId, m.BroadcastId, m.VariantId, m.VariantId, threshold)
}
