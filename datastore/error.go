package datastore

import "fmt"

func failedTo(s string, err error) error {
	return fmt.Errorf("failed to %s: %s", s, err)
}
