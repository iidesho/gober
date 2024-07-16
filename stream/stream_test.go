package stream_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/iidesho/gober/stream/event/store/ondisk"
	"github.com/gofrs/uuid"
	jsoniter "github.com/json-iterator/go"

	"github.com/iidesho/bragi/sbragi"
)

var json = jsoniter.ConfigDefault

var es stream.FilteredStream[[]byte]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

type md struct {
	Extra bcts.SmallBytes `json:"extra"`
}

type dd struct {
	Name string `json:"name"`
	Id   int    `json:"id"`
}

func TestInit(t *testing.T) {
	//log, _ := sbragi.NewDebugLogger()
	//log.SetDefault()
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	pers, err := ondisk.Init(STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	est, err := stream.Init[[]byte](pers, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	es = est
}

var extraKey = bcts.TinyString("extra")

func TestStoreOrder(t *testing.T) {
	writer := es.Write()
	for i := 1; i <= 5; i++ {
		data := dd{
			Id:   i,
			Name: "test",
		}
		meta := md{
			Extra: bcts.SmallBytes("extra metadata test"),
		}
		bdata, err := json.Marshal(data)
		if err != nil {
			t.Error(err)
			return
		}
		sbragi.Debug(string(bdata))
		e, err := event.NewBuilder().
			WithType(event.Created).
			WithData(bdata).
			WithMetadata(event.Metadata{
				Extra: map[bcts.TinyString]bcts.SmallBytes{
					extraKey: meta.Extra,
				},
			}).
			BuildStore()
		if err != nil {
			t.Error(err)
			return
		}
		we := event.NewWriteEvent(*e.Event())
		writer <- we
		<-we.Done()
		/*
			_, err = es.Store(e)
			if err != nil {
				t.Error(err)
				return
			}
		*/
	}
}

func TestStreamOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := es.Stream(
		[]event.Type{event.Created},
		store.STREAM_START,
		stream.ReadEventType(event.Created),
		ctx,
	)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 1; i <= 5; i++ {
		e := <-stream
		var data dd
		err = json.Unmarshal(e.Data, &data)
		sbragi.WithError(err).Debug("stream data", "i", i, "event", e, "data", data)
		if err != nil {
			log.Fatal("I AM HIT!")
			t.Error(err, string(e.Data))
			t.Fail()
			t.Fatal(err)
			return
		}
		if e.Type != event.Created {
			t.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if data.Id != i {
			t.Error(fmt.Errorf("missmatch event data id"))
			return
		}
		if data.Name != "test" {
			t.Error(fmt.Errorf("missmatch event data name"))
			return
		}
		if string(e.Metadata.Extra[extraKey]) != "extra metadata test" {
			t.Error(fmt.Errorf("missmatch event metadata extra"))
			return
		}
		if e.Metadata.EventType != e.Type {
			t.Error(fmt.Errorf("missmatch event metadata type and event type"))
			return
		}
	}
}

func TestStoreDuplicate(t *testing.T) {
	for i := 0; i < 2; i++ {
		data := dd{
			Id:   i,
			Name: "test",
		}
		meta := md{
			Extra: bcts.SmallBytes("extra metadata test"),
		}
		bdata, err := json.Marshal(data)
		if err != nil {
			t.Error(err)
			return
		}
		e, err := event.NewBuilder().
			WithType(event.Created).
			WithData(bdata).
			WithMetadata(event.Metadata{
				Extra: map[bcts.TinyString]bcts.SmallBytes{
					extraKey: meta.Extra,
				},
			}).
			BuildStore()
		if err != nil {
			t.Error(err)
			return
		}
		_, err = es.Store(*e.Event())
		if err != nil {
			t.Error(err)
			return
		}
	}
}

func TestStreamDuplicate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := es.Stream(
		[]event.Type{event.Created},
		store.STREAM_START,
		stream.ReadEventType(event.Created),
		ctx,
	)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 1; i <= 5; i++ {
		e := <-stream
		var data dd
		err = json.Unmarshal(e.Data, &data)
		if err != nil {
			t.Error(err)
			return
		}
		if e.Type != event.Created {
			t.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if data.Id != i {
			t.Error(fmt.Errorf("missmatch event data id"))
			return
		}
		if data.Name != "test" {
			t.Error(fmt.Errorf("missmatch event data name"))
			return
		}
		if string(e.Metadata.Extra[extraKey]) != "extra metadata test" {
			t.Error(fmt.Errorf("missmatch event metadata extra"))
			return
		}
		if e.Metadata.EventType != e.Type {
			t.Error(fmt.Errorf("missmatch event metadata type and event type"))
			return
		}
	}
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}

func BenchmarkStoreAndStream(b *testing.B) {
	//log.SetLevel(log.ERROR) TODO: should add to sbragi
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pers, err := ondisk.Init(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	est, err := stream.Init[[]byte](pers, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	events := make([]event.WriteEventReadStatus[[]byte], b.N)
	for i := 0; i < b.N; i++ {
		data := dd{
			Id:   i,
			Name: "test" + b.Name(),
		}
		meta := md{
			Extra: bcts.SmallBytes("extra metadata test"),
		}
		bdata, err := json.Marshal(data)
		if err != nil {
			b.Error(err)
			return
		}
		e, err := event.NewBuilder().
			WithType(event.Created).
			WithData(bdata).
			WithMetadata(event.Metadata{
				Extra: map[bcts.TinyString]bcts.SmallBytes{
					extraKey: meta.Extra,
				},
			}).
			BuildStore()
		if err != nil {
			b.Error(err)
			return
		}
		events[i] = event.NewWriteEvent(*e.Event())
	}
	stream, err := est.Stream(
		[]event.Type{event.Created},
		store.STREAM_START,
		stream.ReadAll(),
		ctx,
	)
	b.ResetTimer()
	writer := est.Write()
	for i := 0; i < b.N; i++ {
		writer <- events[i]
		status := <-events[i].Done()
		if status.Error != nil {
			b.Error(status.Error)
			return
		}
	}
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		e := <-stream
		if e.Type != event.Created {
			b.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if !bytes.Equal(e.Data, events[i].Event().Data) {
			b.Error(fmt.Errorf("missmatch event data, %v != %v", e.Data, events[i].Event().Data))
			return
		}
	}
}
