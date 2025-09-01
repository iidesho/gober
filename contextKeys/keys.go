package contextkeys

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

const (
	TraceID ContextKey = "trace_id"
	SpanID  ContextKey = "span_id"
)

var Keys []ContextKey = []ContextKey{
	TraceID,
	SpanID,
}
