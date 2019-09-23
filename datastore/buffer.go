package datastore

import (
	"bytes"

	"github.com/mailru/dbr"
)

type QueryBuffer struct {
	bytes.Buffer
	v []interface{}
}

func NewQueryBuffer() dbr.Buffer {
	return &QueryBuffer{}
}

func (b *QueryBuffer) WriteValue(v ...interface{}) error {
	b.v = append(b.v, v...)
	return nil
}

func (b *QueryBuffer) Value() []interface{} {
	return b.v
}
