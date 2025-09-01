package bcts

import (
	"context"
	"io"

	contextkeys "github.com/iidesho/gober/contextKeys"
	"go.opentelemetry.io/otel/trace"
)

func WriteContext(w io.Writer, ctx context.Context) error {
	values := map[contextkeys.ContextKey]any{}
	for _, key := range contextkeys.Keys {
		if val := ctx.Value(key); val != nil {
			values[key] = val
		}
	}
	spanCTX := trace.SpanContextFromContext(ctx)
	if spanCTX.IsValid() {
		values["otel_trace"] = "true"
		values["trace_id"] = spanCTX.TraceID().String()
		values["span_id"] = spanCTX.SpanID().String()
	}
	return WriteMapAny(w, values)
}

func ReadContext(r io.Reader, ctx context.Context) (context.Context, error) {
	values := map[contextkeys.ContextKey]any{}
	err := ReadMapAny(r, &values)
	if err != nil {
		return nil, err
	}
	for key, val := range values {
		ctx = context.WithValue(ctx, key, val)
	}
	if v, ok := values["otel_trace"]; ok && v == "true" {
		spanCTX := trace.NewSpanContext(trace.SpanContextConfig{})
		if traceID, ok := values["trace_id"].(string); ok {
			if traceID, err := trace.SpanIDFromHex(traceID); err == nil {
				spanCTX = spanCTX.WithSpanID(traceID)
			}
		}
		if spanID, ok := values["span_id"].(string); ok {
			if spanID, err := trace.SpanIDFromHex(spanID); err == nil {
				spanCTX = spanCTX.WithSpanID(spanID)
			}
		}
		ctx = trace.ContextWithSpanContext(ctx, spanCTX)
	}
	return ctx, nil
}
