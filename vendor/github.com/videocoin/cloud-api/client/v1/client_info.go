package v1

import (
	"reflect"
	"strings"
)

type serviceClientInfo struct {
	Name string
	Addr string
}

func gatherServiceClientInfo(spec interface{}) []serviceClientInfo {
	s := reflect.ValueOf(spec)

	if s.Kind() != reflect.Ptr {
		return nil
	}

	s = s.Elem()

	if s.Kind() != reflect.Struct {
		return nil
	}

	typeOfSpec := s.Type()

	infos := make([]serviceClientInfo, 0, s.NumField())
	for i := 0; i < s.NumField(); i++ {
		ftype := typeOfSpec.Field(i)
		if ftype.Type.Kind() != reflect.String {
			continue
		}

		name := ftype.Tag.Get("envconfig")

		if name == "-" || name == "" || !strings.HasSuffix(name, "_RPC_ADDR") {
			continue
		}

		f := s.Field(i)
		addr := f.String()

		name = strings.ToLower(strings.TrimSuffix(name, "_RPC_ADDR"))

		if addr == "" {
			addr = ftype.Tag.Get("default")
		}

		infos = append(infos, serviceClientInfo{
			Name: name,
			Addr: addr,
		})
	}

	return infos
}
