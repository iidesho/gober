package bcts_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/gober/bcts"
	contextkeys "github.com/iidesho/gober/contextKeys"
	"github.com/iidesho/gober/traces"
	"github.com/iidesho/gober/webserver/health"
	"go.opentelemetry.io/otel/trace"
)

func TestContext(t *testing.T) {
	health.Name = "gober"
	traces.Init()
	buf := bytes.NewBuffer(make([]byte, 0))
	var err error
	dead := time.Now().Add(time.Second * 10)
	ctx := context.WithValue(context.Background(), contextkeys.Deadline, dead)
	ctxSpan, childSpan := traces.Traces.Start(
		ctx,
		"context_test",
	)
	defer childSpan.End()
	spanCTX := trace.SpanContextFromContext(ctxSpan)
	fmt.Println(spanCTX)
	err = bcts.WriteContext(buf, ctxSpan)
	if err != nil {
		t.Fatal(err)
	}
	// fmt.Println(buf.String())
	// var tc2 uint16
	// err = bcts.ReadUInt16(buf, &tc2)
	ctx2, err := bcts.ReadContext(buf, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if gotDead := ctx2.Value(contextkeys.Deadline); gotDead == nil ||
		!dead.Equal(gotDead.(time.Time)) {
		t.Fatalf("not equal read and write %s != %s", gotDead, dead)
		// t.Fatalf("not equal read and write %d != %d", tc, tc2)
	}
	buf.Reset()
}

func BenchmarkContext(b *testing.B) {
	buf := bytes.NewBuffer(make([]byte, 1024))
	var err error
	tID := uuid.Must(uuid.NewV7())
	ctx := context.WithValue(context.Background(), contextkeys.TraceID, tID)
	for range b.N {
		err = bcts.WriteContext(buf, ctx)
		if err != nil {
			b.Fatal(err)
		}
		ctx2, err := bcts.ReadContext(buf, context.Background())
		if err != nil {
			b.Fatal(err)
		}
		if traceID := ctx2.Value(contextkeys.TraceID); traceID == nil || traceID != tID {
			b.Fatalf("not equal read and write %s != %s", traceID, tID)
		}
		buf.Reset()
	}
}
