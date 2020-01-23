package eventbus

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	pstreamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-pkg/mqmux"
	tracerext "github.com/videocoin/cloud-pkg/tracer"
)

type Config struct {
	Logger  *logrus.Entry
	URI     string
	Name    string
	DM      *datastore.DataManager
	Streams pstreamsv1.StreamsServiceClient
	Miners  minersv1.MinersServiceClient
}

type EventBus struct {
	logger  *logrus.Entry
	mq      *mqmux.WorkerMux
	dm      *datastore.DataManager
	streams pstreamsv1.StreamsServiceClient
	miners  minersv1.MinersServiceClient
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
		logger:  c.Logger,
		mq:      mq,
		dm:      c.DM,
		streams: c.Streams,
		miners:  c.Miners,
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

	req := new(pstreamsv1.Event)
	err = json.Unmarshal(d.Body, req)
	if err != nil {
		tracerext.SpanLogError(span, err)
		return err
	}

	span.SetTag("stream_id", req.StreamID)
	span.SetTag("event_type", req.Type.String())

	logger := e.logger.WithFields(logrus.Fields{
		"stream_id":  req.StreamID,
		"event_type": req.Type.String(),
	})
	logger.Debugf("handling request %+v", req)

	ctx := opentracing.ContextWithSpan(context.Background(), span)

	switch req.Type {
	case pstreamsv1.EventTypeUpdate:
		{
			logger.Info("getting stream")

			streamReq := &pstreamsv1.StreamRequest{Id: req.StreamID}
			streamResp, err := e.streams.Get(ctx, streamReq)
			if err != nil {
				tracerext.SpanLogError(span, err)
				fmtErr := fmt.Errorf("failed to get stream: %s", err)
				logger.Error(fmtErr)
				return fmtErr
			}

			switch streamResp.Status {
			case streamsv1.StreamStatusPending:
				{
					logger.Info("stream is pending")
					return e.onStreamStatusPending(ctx, streamResp)
				}
			case streamsv1.StreamStatusCompleted:
				{
					logger.Info("stream is completed")
					return e.onStreamStatusCompleted(ctx, streamResp)
				}
			}
		}
	case pstreamsv1.EventTypeDelete:
		{
			streamReq := &pstreamsv1.StreamRequest{Id: req.StreamID}
			streamResp, err := e.streams.Get(ctx, streamReq)
			if err != nil {
				tracerext.SpanLogError(span, err)
				return fmt.Errorf("failed to get stream: %s", err)
			}

			if streamResp.InputType != streamsv1.InputTypeFile {
				e.logger.Info("deleting task")

				atReq := &minersv1.AssignTaskRequest{
					ClientID: "",
					TaskID:   req.StreamID,
				}

				e.logger.WithFields(logrus.Fields{
					"client_id": atReq.ClientID,
					"task_id":   atReq.TaskID,
				}).Info("unassigning task")

				_, err = e.miners.UnassignTask(context.Background(), atReq)
				if err != nil {
					fmtErr := fmt.Errorf("failed to call unassign task: %s", err)
					tracerext.SpanLogError(span, fmtErr)
				}
			}
		}
	case pstreamsv1.EventTypeUnknown:
		e.logger.Error("event type is unknown")
	}

	return nil
}

func (e *EventBus) onStreamStatusPending(
	ctx context.Context,
	stream *pstreamsv1.StreamResponse,
) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "onStreamStatusPending")
	defer span.Finish()

	logger := e.logger.WithField("stream_id", stream.ID)

	logger.Info("creating tasks")
	_, err := e.dm.CreateTasksFromStreamResponse(spanCtx, stream)
	if err != nil {
		fmtErr := fmt.Errorf("failed to creating tasks: %s", err)
		tracerext.SpanLogError(span, fmtErr)
		logger.Error(fmtErr)
		return err
	}

	return nil
}

func (e *EventBus) onStreamStatusCompleted(
	ctx context.Context,
	stream *pstreamsv1.StreamResponse,
) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "onStreamStatusCompleted")
	defer span.Finish()

	logger := e.logger.WithField("stream_id", stream.ID)

	task, err := e.dm.GetTaskByID(spanCtx, stream.ID)
	if err != nil {
		tracerext.SpanLogError(span, err)
		return err
	}

	defer func() {
		atReq := &minersv1.AssignTaskRequest{
			ClientID: task.ClientID.String,
			TaskID:   task.ID,
		}

		logger.WithFields(logrus.Fields{
			"client_id": atReq.ClientID,
			"task_id":   atReq.TaskID,
		}).Info("unassigning task")

		_, err = e.miners.UnassignTask(context.Background(), atReq)
		if err != nil {
			fmtErr := fmt.Errorf("unassign task to miners service: %s", err)
			tracerext.SpanLogError(span, fmtErr)
			logger.Error(fmtErr)
		}
	}()

	logger.Info("marking task as completed")

	err = e.dm.MarkTaskAsCompleted(ctx, task)
	if err != nil {
		fmtErr := fmt.Errorf("failed to mark task as completed: %s", err)
		tracerext.SpanLogError(span, fmtErr)
		logger.Error(fmtErr)
		return fmtErr
	}

	return nil
}
