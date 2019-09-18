package rpc

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

func logFailedTo(logger *logrus.Entry, msg string, err error) {
	fmtErr := fmt.Errorf("failed to %s: %s", msg, err.Error())
	if msg == "" {
		fmtErr = err
	}
	logger.Error(fmtErr)
}
