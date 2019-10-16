package v1

import (
	"database/sql/driver"
	"errors"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

func (input TaskInput) Value() (driver.Value, error) {
	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	b, err := m.Marshal(input)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (input *TaskInput) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed.")
	}

	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	return m.Unmarshal(source, input)
}

func (output TaskOutput) Value() (driver.Value, error) {
	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	b, err := m.Marshal(output)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (output *TaskOutput) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed.")
	}

	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: true}
	return m.Unmarshal(source, output)
}

func (s TaskStatus) Value() (driver.Value, error) {
	return TaskStatus_name[int32(s)], nil
}

func (s *TaskStatus) Scan(src interface{}) error {
	sID, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed.")
	}

	*s = TaskStatus(TaskStatus_value[string(sID)])

	return nil
}
