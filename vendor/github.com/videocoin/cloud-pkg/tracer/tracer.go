package tracer

import (
	"io"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"github.com/streadway/amqp"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics"
	"github.com/videocoin/cloud-pkg/mqmux"
)

func NewTracer(serviceName string) (io.Closer, error) {
	cfg, err := config.FromEnv()
	if err != nil {
		return nil, err
	}

	cfg.Sampler.Type = jaeger.SamplerTypeConst
	cfg.Sampler.Param = 1
	// set JAEGER_REPORTER_LOG_SPANS to enable
	cfg.Reporter.LogSpans = false

	return cfg.InitGlobalTracer(
		serviceName,
		config.Metrics(metrics.NullFactory),
		config.Logger(jaeger.StdLogger),
	)

}

func ExtractMQSpan(d amqp.Delivery, spanName string) (opentracing.SpanContext, opentracing.Span) {
	tracer := opentracing.GlobalTracer()
	ctx, err := tracer.Extract(opentracing.TextMap, mqmux.RMQHeaderCarrier(d.Headers))

	var span opentracing.Span
	if err != nil {
		span = tracer.StartSpan(spanName)
	} else {
		span = tracer.StartSpan(spanName, ext.RPCServerOption(ctx))
	}

	return ctx, span
}

func SpanLogError(span opentracing.Span, err error) {
	if err != nil {
		ext.Error.Set(span, true)
		span.LogFields(
			log.String("event", "error"),
			log.String("message", err.Error()),
		)
	}
}
