package v1

import (
	"database/sql/driver"
	"errors"
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
