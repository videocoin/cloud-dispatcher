package eventbus

import (
	"encoding/json"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	privatev1 "github.com/videocoin/cloud-api/streams/private/v1"
	"github.com/videocoin/cloud-pkg/mqmux"
	tracerext "github.com/videocoin/cloud-pkg/tracer"
)

type Config struct {
	Logger *logrus.Entry
	URI    string
	Name   string
}

type EventBus struct {
	logger *logrus.Entry
	mq     *mqmux.WorkerMux
}

func New(c *Config) (*EventBus, error) {
	mq, err := mqmux.NewWorkerMux(c.URI, c.Name)
	if err != nil {
		return nil, err
	}
	if c.Logger != nil {
		mq.Logger = c.Logger
	}

	return &EventBus{
		logger: c.Logger,
		mq:     mq,
	}, nil
}

func (e *EventBus) Start() error {
	err := e.mq.Consumer("streams.events", 1, false, e.handleStreamEvent)
	if err != nil {
		return err
	}
	return e.mq.Run()
}

func (e *EventBus) Stop() error {
	return e.mq.Close()
}

func (e *EventBus) handleStreamEvent(d amqp.Delivery) error {
	var span opentracing.Span
	tracer := opentracing.GlobalTracer()
	spanCtx, err := tracer.Extract(opentracing.TextMap, mqmux.RMQHeaderCarrier(d.Headers))

	e.logger.Debugf("handling body: %+v", string(d.Body))

	if err != nil {
		span = tracer.StartSpan("eventbus.handleStreamEvent")
	} else {
		span = tracer.StartSpan("eventbus.handleStreamEvent", ext.RPCServerOption(spanCtx))
	}

	defer span.Finish()

	req := new(privatev1.Event)
	err = json.Unmarshal(d.Body, req)
	if err != nil {
		tracerext.SpanLogError(span, err)
		return err
	}

	span.SetTag("stream_id", req.StreamID)
	span.SetTag("event_type", req.Type.String())

	e.logger.Debugf("handling request %+v", req)

	return nil
}
