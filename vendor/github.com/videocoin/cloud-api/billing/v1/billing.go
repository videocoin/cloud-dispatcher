package v1

import (
	"database/sql/driver"
	"errors"
)

func (s TransactionStatus) Value() (driver.Value, error) {
	return TransactionStatus_name[int32(s)], nil
}

func (s *TransactionStatus) Scan(src interface{}) error {
	sid, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed")
	}

	*s = TransactionStatus(TransactionStatus_value[string(sid)])

	return nil
}
