package v1

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

func (worker WorkerResponse) Value() (driver.Value, error) {
	b, err := json.Marshal(worker)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

func (worker *WorkerResponse) Scan(src interface{}) error {
	source, ok := src.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed")
	}

	return json.Unmarshal(source, worker)
}
