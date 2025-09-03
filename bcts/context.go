package bcts

import (
	"context"
	"io"

	contextkeys "github.com/iidesho/gober/contextKeys"
	"github.com/iidesho/gober/itr"
	"go.opentelemetry.io/otel/trace"
)

func WriteContext(w io.Writer, ctx context.Context) error {
	values := map[string]any{}
	for _, key := range contextkeys.Keys {
		if val := ctx.Value(key); val != nil {
			values[key.String()] = val
		}
	}
	spanCTX := trace.SpanContextFromContext(ctx)
	if spanCTX.IsValid() {
		values["otel_trace"] = "true"
		values["trace_id"] = spanCTX.TraceID().String()
		values["span_id"] = spanCTX.SpanID().String()
	}
	return WriteMapAny(w, values) //, WriteTinyString, WriteAny)
}

func ReadContext(r io.Reader, ctx context.Context) (context.Context, error) {
	values := map[string]any{}
	// err := ReadMapAnyFunc(r, &values, ReadTinyString, ReadAny[uuid.UUID])
	err := ReadMapAny(r, &values)
	if err != nil {
		return nil, err
	}
	if v, ok := values["otel_trace"]; ok && v == "true" {
		delete(values, "otel_trace")
		spanCTX := trace.NewSpanContext(trace.SpanContextConfig{})
		if traceID, ok := values["trace_id"].(string); ok {
			if traceID, err := trace.TraceIDFromHex(traceID); err == nil {
				delete(values, "trace_id")
				spanCTX = spanCTX.WithTraceID(traceID)
			}
		}
		if spanID, ok := values["span_id"].(string); ok {
			if spanID, err := trace.SpanIDFromHex(spanID); err == nil {
				delete(values, "span_id")
				spanCTX = spanCTX.WithSpanID(spanID)
			}
		}
		ctx = trace.ContextWithSpanContext(ctx, spanCTX)
	}
	for key, val := range values {
		k := itr.NewIterator(contextkeys.Keys).Filter(func(s contextkeys.ContextKey) bool {
			return s.String() == key
		}).First()
		if k.String() != "" {
			ctx = context.WithValue(ctx, k, val)
		} else {
			ctx = context.WithValue(ctx, key, val)
		}
	}
	return ctx, nil
}
