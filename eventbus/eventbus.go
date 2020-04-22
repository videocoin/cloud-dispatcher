package eventbus

import (
	"context"
	"encoding/json"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/streadway/amqp"
	clientv1 "github.com/videocoin/cloud-api/client/v1"
	v1 "github.com/videocoin/cloud-api/dispatcher/v1"
	minersv1 "github.com/videocoin/cloud-api/miners/v1"
	pstreamsv1 "github.com/videocoin/cloud-api/streams/private/v1"
	streamsv1 "github.com/videocoin/cloud-api/streams/v1"
	"github.com/videocoin/cloud-dispatcher/datastore"
	"github.com/videocoin/cloud-pkg/mqmux"
	tracerext "github.com/videocoin/cloud-pkg/tracer"
	"go.uber.org/zap"
)

type EventBus struct {
	logger *zap.Logger
	mq     *mqmux.WorkerMux
	dm     *datastore.DataManager
	sc     *clientv1.ServiceClient
}

func NewEventBus(ctx context.Context, uri, name string, dm *datastore.DataManager, sc *clientv1.ServiceClient) (*EventBus, error) {
	logger := ctxzap.Extract(ctx).With(zap.String("system", "eventbus"))

	mq, err := mqmux.NewWorkerMux(uri, name)
	if err != nil {
		return nil, err
	}

	return &EventBus{
		logger: logger,
		mq:     mq,
		dm:     dm,
		sc:     sc,
	}, nil
}

func (e *EventBus) Start() error {
	err := e.mq.Consumer("streams.events", 1, false, e.handleStreamEvent)
	if err != nil {
		return err
	}

	err = e.mq.Consumer("tasks.events", 1, false, e.handleTaskEvent)
	if err != nil {
		return err
	}

	err = e.mq.Publisher("dispatcher.events")
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

	e.logger.Debug("handling body", zap.String("body", string(d.Body)))

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

	logger := e.logger.With(
		zap.String("stream_id", req.StreamID),
		zap.String("event_type", req.Type.String()),
	)

	ctx := opentracing.ContextWithSpan(context.Background(), span)

	switch req.Type {
	case pstreamsv1.EventTypeUpdate:
		{
			logger.Info("getting stream")

			streamReq := &pstreamsv1.StreamRequest{Id: req.StreamID}
			streamResp, err := e.sc.Streams.Get(ctx, streamReq)
			if err != nil {
				logger.Error("failed to get stream", zap.Error(err))
				return nil
			}

			switch streamResp.Status {
			case streamsv1.StreamStatusPending:
				{
					logger.Info("stream is pending")
					return e.onStreamStatusPending(ctx, streamResp)
				}
			// case streamsv1.StreamStatusCompleted:
			// 	{
			// 		logger.Info("stream is completed")
			// 		return e.onStreamStatusCompleted(ctx, streamResp)
			// 	}
			case streamsv1.StreamStatusCancelled:
				logger.Info("stream was cancelled")
				return e.onStreamStatusCancelled(ctx, streamResp)
			}
		}
	case pstreamsv1.EventTypeDelete:
		{
			streamReq := &pstreamsv1.StreamRequest{Id: req.StreamID}
			streamResp, err := e.sc.Streams.Get(ctx, streamReq)
			if err != nil {
				logger.Error("failed to get stream", zap.Error(err))
				return nil
			}

			if streamResp.InputType != streamsv1.InputTypeFile {
				e.logger.Info("deleting task")

				atReq := &minersv1.AssignTaskRequest{
					ClientID: "",
					TaskID:   req.StreamID,
				}

				_, err = e.sc.Miners.UnassignTask(context.Background(), atReq)
				if err != nil {
					logger.Error("failed to unassign task from miner", zap.Error(err))
				}
			}
		}
	case pstreamsv1.EventTypeUnknown:
		e.logger.Error("event type is unknown")
	}

	return nil
}

func (e *EventBus) handleTaskEvent(d amqp.Delivery) error {
	var span opentracing.Span
	tracer := opentracing.GlobalTracer()
	spanCtx, err := tracer.Extract(opentracing.TextMap, mqmux.RMQHeaderCarrier(d.Headers))

	e.logger.Debug("handling body", zap.String("body", string(d.Body)))

	if err != nil {
		span = tracer.StartSpan("eventbus.handleTaskEvent")
	} else {
		span = tracer.StartSpan("eventbus.handleTaskEvent", ext.RPCServerOption(spanCtx))
	}

	defer span.Finish()

	req := new(v1.Event)
	err = json.Unmarshal(d.Body, req)
	if err != nil {
		tracerext.SpanLogError(span, err)
		return err
	}

	span.SetTag("task_id", req.TaskID)
	span.SetTag("event_type", req.Type.String())

	logger := e.logger.With(
		zap.String("stream_id", req.TaskID),
		zap.String("event_type", req.Type.String()),
	)

	ctx := opentracing.ContextWithSpan(context.Background(), span)

	switch req.Type {
	case v1.EventTypeUpdateStatus:
		{
			logger.Info("getting task")

			task, err := e.dm.GetTaskByID(ctx, req.TaskID)
			if err != nil {
				logger.Error("failed to get task", zap.Error(err))
				return err
			}

			if req.Status == v1.TaskStatusPending {
				err = e.dm.MarkTaskAsPending(ctx, task)
				if err != nil {
					logger.Error("failed to mark task as pending", zap.Error(err))
					return err
				}
			}

			err = e.dm.ClearClientID(ctx, task)
			if err != nil {
				logger.Error("failed to clear client id", zap.Error(err))
				return err
			}
		}
	case v1.EventTypeUnknown:
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

	logger := e.logger.With(zap.String("stream_id", stream.ID))

	logger.Info("creating tasks")
	_, err := e.dm.CreateTasksFromStreamResponse(spanCtx, stream)
	if err != nil {
		logger.Error("failed to creating tasks", zap.Error(err))
		return err
	}

	return nil
}

func (e *EventBus) onStreamStatusCancelled(
	ctx context.Context,
	stream *pstreamsv1.StreamResponse,
) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "onStreamStatusCancelled")
	defer span.Finish()

	logger := e.logger.With(zap.String("stream_id", stream.ID))

	tasks, err := e.dm.GetTasksByStreamID(spanCtx, stream.ID)
	if err != nil {
		tracerext.SpanLogError(span, err)
		return err
	}

	defer func() {
		for _, task := range tasks {
			atReq := &minersv1.AssignTaskRequest{
				ClientID: task.ClientID.String,
				TaskID:   task.ID,
			}

			logger.Info(
				"unassigning task",
				zap.String("client_id", atReq.ClientID),
				zap.String("task_id", atReq.TaskID),
			)

			_, err = e.sc.Miners.UnassignTask(context.Background(), atReq)
			if err != nil {
				logger.Error("failed to unassign task from miner", zap.Error(err))
			}
		}
	}()

	logger.Info("cancelling tasks")

	for _, task := range tasks {
		err = e.dm.MarkTaskAsCanceled(ctx, task)
		if err != nil {
			logger.Error("failed to mark task as cancelled", zap.Error(err))
			continue
		}
	}

	return nil
}

// func (e *EventBus) onStreamStatusCompleted(
// 	ctx context.Context,
// 	stream *pstreamsv1.StreamResponse,
// ) error {
// 	span, spanCtx := opentracing.StartSpanFromContext(ctx, "onStreamStatusCompleted")
// 	defer span.Finish()

// 	logger := e.logger.With(zap.String("stream_id", stream.ID))

// 	task, err := e.dm.GetTaskByID(spanCtx, stream.ID)
// 	if err != nil {
// 		tracerext.SpanLogError(span, err)
// 		return err
// 	}

// 	defer func() {
// 		atReq := &minersv1.AssignTaskRequest{
// 			ClientID: task.ClientID.String,
// 			TaskID:   task.ID,
// 		}

// 		logger.Info(
// 			"unassigning task",
// 			zap.String("client_id", atReq.ClientID),
// 			zap.String("task_id", atReq.TaskID),
// 		)

// 		_, err = e.sc.Miners.UnassignTask(context.Background(), atReq)
// 		if err != nil {
// 			logger.Error("failed to unassign task from miner", zap.Error(err))
// 		}
// 	}()

// 	logger.Info("marking task as completed")

// 	err = e.dm.MarkTaskAsCompleted(ctx, task)
// 	if err != nil {
// 		logger.Error("failed to mark task as completed", zap.Error(err))
// 		return nil
// 	}

// 	return nil
// }

func (e *EventBus) EmitTaskCompleted(
	ctx context.Context,
	task *datastore.Task,
	miner *minersv1.MinerResponse,
) error {
	headers := make(amqp.Table)

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		ext.SpanKindRPCServer.Set(span)
		ext.Component.Set(span, "transcoder")
		err := span.Tracer().Inject(
			span.Context(),
			opentracing.TextMap,
			mqmux.RMQHeaderCarrier(headers),
		)
		if err != nil {
			e.logger.Error("failed to span inject", zap.Error(err))
		}
	}

	profile, err := e.dm.GetProfile(ctx, task.ProfileID)
	if err != nil {
		return err
	}

	stream, err := e.dm.GetStream(ctx, task.StreamID)
	if err != nil {
		return err
	}

	if task.Output != nil && task.Output.Num > 0 && task.Output.Duration > 0 {
		event := &v1.Event{
			Type:                  v1.EventTypeTaskCompleted,
			TaskID:                task.ID,
			StreamID:              stream.ID,
			StreamName:            stream.Name,
			StreamContractAddress: task.StreamContractAddress.String,
			StreamIsLive:          false,
			ProfileID:             task.ProfileID,
			ProfileCost:           profile.Cost,
			ProfileName:           profile.Name,
			ClientID:              task.ClientID.String,
			ClientUserID:          miner.UserID,
			UserID:                task.UserID.String,
			ChunkNum:              uint64(task.Output.Num),
			Duration:              task.Output.Duration,
			CostPerSec:            profile.Cost / 60,
		}

		err := e.mq.PublishX("dispatcher.events", event, headers)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *EventBus) EmitSegmentTranscoded(
	ctx context.Context,
	req *v1.TaskSegmentRequest,
	task *datastore.Task,
	miner *minersv1.MinerResponse,
) error {
	headers := make(amqp.Table)

	span := opentracing.SpanFromContext(ctx)
	if span != nil {
		ext.SpanKindRPCServer.Set(span)
		ext.Component.Set(span, "transcoder")
		err := span.Tracer().Inject(
			span.Context(),
			opentracing.TextMap,
			mqmux.RMQHeaderCarrier(headers),
		)
		if err != nil {
			e.logger.Error("failed to span inject", zap.Error(err))
		}
	}

	profile, err := e.dm.GetProfile(ctx, task.ProfileID)
	if err != nil {
		return err
	}

	stream, err := e.dm.GetStream(ctx, task.StreamID)
	if err != nil {
		return err
	}

	if req.Num > 0 && req.Duration > 0 {
		event := &v1.Event{
			Type:                  v1.EventTypeSegementTranscoded,
			TaskID:                task.ID,
			StreamID:              stream.ID,
			StreamName:            stream.Name,
			StreamContractAddress: task.StreamContractAddress.String,
			StreamIsLive:          true,
			ProfileID:             task.ProfileID,
			ProfileCost:           profile.Cost,
			ProfileName:           profile.Name,
			ClientID:              task.ClientID.String,
			ClientUserID:          miner.UserID,
			UserID:                task.UserID.String,
			ChunkNum:              req.Num,
			Duration:              req.Duration,
			CostPerSec:            profile.Cost / 60,
		}

		err := e.mq.PublishX("dispatcher.events", event, headers)
		if err != nil {
			return err
		}
	}

	return nil
}
