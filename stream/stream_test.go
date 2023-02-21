package stream

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/gofrs/uuid"

	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
	"github.com/cantara/gober/stream/event/store/inmemory"
)

var es FilteredStream
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

type md struct {
	Extra string `json:"extra"`
}

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func TestInit(t *testing.T) {
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	pers, err := inmemory.Init(STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	est, err := Init(pers, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	es = est
	return
}

func TestStoreOrder(t *testing.T) {
	writer := es.Write()
	for i := 1; i <= 5; i++ {
		data := dd{
			Id:   i,
			Name: "test",
		}
		meta := md{
			Extra: "extra metadata test",
		}
		bdata, err := json.Marshal(data)
		if err != nil {
			t.Error(err)
			return
		}
		e, err := event.NewBuilder().
			WithType(event.Create).
			WithData(bdata).
			WithMetadata(event.Metadata{
				Extra: map[string]any{"extra": meta.Extra},
			}).
			BuildStore()
		if err != nil {
			t.Error(err)
			return
		}
		writer <- e
		/*
			_, err = es.Store(e)
			if err != nil {
				t.Error(err)
				return
			}
		*/
	}
	return
}

func TestStreamOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := es.Stream([]event.Type{event.Create}, store.STREAM_START, ReadEventType(event.Create), ctx)
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
		if e.Type != event.Create {
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
		if e.Metadata.Extra["extra"] != "extra metadata test" {
			t.Error(fmt.Errorf("missmatch event metadata extra"))
			return
		}
		if e.Metadata.EventType != e.Type {
			t.Error(fmt.Errorf("missmatch event metadata type and event type"))
			return
		}
	}
	return
}

func TestStoreDuplicate(t *testing.T) {
	for i := 0; i < 2; i++ {
		data := dd{
			Id:   i,
			Name: "test",
		}
		meta := md{
			Extra: "extra metadata test",
		}
		bdata, err := json.Marshal(data)
		if err != nil {
			t.Error(err)
			return
		}
		e, err := event.NewBuilder().
			WithType(event.Create).
			WithData(bdata).
			WithMetadata(event.Metadata{
				Extra: map[string]any{"extra": meta.Extra},
			}).
			BuildStore()
		if err != nil {
			t.Error(err)
			return
		}
		_, err = es.Store(event.ByteEvent(e.Event))
		if err != nil {
			t.Error(err)
			return
		}
	}
	return
}

func TestStreamDuplicate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := es.Stream([]event.Type{event.Create}, store.STREAM_START, ReadEventType(event.Create), ctx)
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
		if e.Type != event.Create {
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
		if e.Metadata.Extra["extra"] != "extra metadata test" {
			t.Error(fmt.Errorf("missmatch event metadata extra"))
			return
		}
		if e.Metadata.EventType != e.Type {
			t.Error(fmt.Errorf("missmatch event metadata type and event type"))
			return
		}
	}
	return
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}

func BenchmarkStoreAndStream(b *testing.B) {
	//log.SetLevel(log.ERROR) TODO: should add to sbragi
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pers, err := inmemory.Init(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	est, err := Init(pers, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	status := make(chan event.WriteStatus, b.N)
	events := make([]event.ByteWriteEvent, b.N)
	for i := 0; i < b.N; i++ {
		data := dd{
			Id:   i,
			Name: "test" + b.Name(),
		}
		meta := md{
			Extra: "extra metadata test",
		}
		bdata, err := json.Marshal(data)
		if err != nil {
			b.Error(err)
			return
		}
		events[i], err = event.NewBuilder().
			WithType(event.Create).
			WithData(bdata).
			WithMetadata(event.Metadata{
				Extra: map[string]any{"extra": meta.Extra},
			}).
			BuildStore()
		if err != nil {
			b.Error(err)
			return
		}
		events[i].Status = status
	}
	writer := est.Write()
	for i := 0; i < b.N; i++ {
		writer <- events[i]
		<-status
		/*
			_, err = est.Store(events[i].Event)
			if err != nil {
				b.Error(err)
				return
			}
		*/
	}
	stream, err := est.Stream([]event.Type{event.Create}, store.STREAM_START, ReadAll(), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		e := <-stream
		if e.Type != event.Create {
			b.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if !bytes.Equal(e.Data, events[i].Data) {
			b.Error(fmt.Errorf("missmatch event data, %v != %v", e.Data, events[i].Data))
			return
		}
	}
}
