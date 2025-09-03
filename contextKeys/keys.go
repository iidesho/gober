package contextkeys

type ContextKey string

func (c ContextKey) String() string {
	return string(c)
}

const (
	Deadline ContextKey = "deadline"
	TraceID  ContextKey = "trace_id"
	SpanID   ContextKey = "span_id"
)

var Keys []ContextKey = []ContextKey{
	Deadline,
	TraceID,
	SpanID,
}
