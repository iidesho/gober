package traces

import (
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/webserver/health"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

var (
	log    = sbragi.WithLocalScope(sbragi.LevelInfo)
	Traces trace.Tracer
)

func Init() {
	Traces = otel.Tracer(health.Name)
}
