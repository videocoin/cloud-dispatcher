package v1

import (
	"database/sql/driver"
	"errors"
)

func (t InputType) Value() (driver.Value, error) {
	return InputType_name[int32(t)], nil
}

func (t *InputType) Scan(src interface{}) error {
	tID, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed.")
	}

	*t = InputType(InputType_value[string(tID)])

	return nil
}

func (t OutputType) Value() (driver.Value, error) {
	return InputType_name[int32(t)], nil
}

func (t *OutputType) Scan(src interface{}) error {
	tID, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed.")
	}

	*t = OutputType(OutputType_value[string(tID)])

	return nil
}
