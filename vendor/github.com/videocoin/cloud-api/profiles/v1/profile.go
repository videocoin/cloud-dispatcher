package v1

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

func (s Spec) Value() (driver.Value, error) {
	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: false}
	b, err := m.Marshal(s)
	return string(b), err
}

func (s *Spec) Scan(value interface{}) error {
	source, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion .([]byte) failed.")
	}

	m := &runtime.JSONPb{OrigName: true, EmitDefaults: true, EnumsAsInts: false}
	return m.Unmarshal(source, s)
}

func (c *Component) Render() string {
	var built []string
	for _, p := range c.Params {
		built = append(built, p.Render())
	}

	return strings.Join(built, " ")
}

func (p *Param) Render() string {
	return p.Key + " " + p.Value
}

func (ct *ComponentType) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(ComponentType_name[int32(*ct)])
	return b, err
}

func (ct *ComponentType) UnmarshalJSON(b []byte) error {
	ctRaw := strings.Trim(string(b), "\"")
	value := ComponentType(ComponentType_value[ctRaw])
	*ct = value

	return nil
}
