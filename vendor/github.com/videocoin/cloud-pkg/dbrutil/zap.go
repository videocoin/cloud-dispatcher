package dbrutil

import (
	"go.uber.org/zap"
)

// ZapEventReceiver is a sentinel EventReceiver; use it if the caller doesn't supply one
type ZapEventReceiver struct {
	logger *zap.Logger
}

func NewZapLogger(logger *zap.Logger) *ZapEventReceiver {
	logger = logger.With(zap.String("system", "sql"))
	return &ZapEventReceiver{
		logger: logger,
	}
}

// Event receives a simple notification when various events occur
func (n *ZapEventReceiver) Event(eventName string) {
	// n.logger.Info(eventName)
}

// EventKv receives a notification when various events occur along with
// optional key/value data
func (n *ZapEventReceiver) EventKv(eventName string, kvs map[string]string) {
	// logger := n.logger
	// for k, v := range kvs {
	// 	logger = logger.With(zap.String(k, v))
	// }
	// logger.Info(eventName)
}

// EventErr receives a notification of an error if one occurs
func (n *ZapEventReceiver) EventErr(eventName string, err error) error {
	n.logger.Error(eventName, zap.Error(err))
	return err
}

// EventErrKv receives a notification of an error if one occurs along with
// optional key/value data
func (n *ZapEventReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	logger := n.logger
	for k, v := range kvs {
		logger = logger.With(zap.String(k, v))
	}
	logger.Error(eventName, zap.Error(err))

	return err
}

// Timing receives the time an event took to happen
func (n *ZapEventReceiver) Timing(eventName string, nanoseconds int64) {
	// ms := nanoseconds / int64(time.Millisecond)
	// n.logger.Info(eventName, zap.Int64("time_ms", ms))
}

// TimingKv receives the time an event took to happen along with optional key/value data
func (n *ZapEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
	// logger := n.logger
	// for k, v := range kvs {
	// 	logger = logger.With(zap.String(k, v))
	// }
	// ms := nanoseconds / int64(time.Millisecond)
	// logger.Info(eventName, zap.Int64("time_ms", ms))
}
