package v1

import (
	"database/sql/driver"
	"errors"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

func (s MinerStatus) Value() (driver.Value, error) {
	return MinerStatus_name[int32(s)], nil
}

func (s *MinerStatus) Scan(src interface{}) error {
	sID, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed.")
	}

	*s = MinerStatus(MinerStatus_value[string(sID)])

	return nil
}

func (c CapacityInfo) Value() (driver.Value, error) {
	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: false}
	b, err := m.Marshal(c)
	return string(b), err
}

func (c *CapacityInfo) Scan(value interface{}) error {
	source, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed.")
	}

	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: false}
	return m.Unmarshal(source, c)
}
