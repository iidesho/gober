package stream

import (
	"context"
	"fmt"
	"github.com/cantara/gober/store/eventstore"
	"github.com/cantara/gober/store/inmemory"
	"testing"

	"github.com/gofrs/uuid"

	log "github.com/cantara/bragi"

	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream/event"
)

var es Stream
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

type md struct {
	Extra string `json:"extra"`
}

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestInit(t *testing.T) {
	pers, err := inmemory.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	est, err := Init(pers, STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	es = est
	return
}

func TestStoreOrder(t *testing.T) {
	for i := 1; i <= 5; i++ {
		data := dd{
			Id:   i,
			Name: "test",
		}
		meta := md{
			Extra: "extra metadata test",
		}
		e, err := event.NewBuilder[dd]().
			WithType(event.Create).
			WithData(data).
			WithMetadata(event.Metadata{
				Extra: map[string]any{"extra": meta.Extra},
			}).
			BuildStore()
		if err != nil {
			t.Error(err)
			return
		}
		_, err = es.Store(e,
			cryptKeyProvider)
		if err != nil {
			t.Error(err)
			return
		}
	}
	return
}

func TestStreamOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := NewStream[dd](es, []event.Type{event.Create}, store.STREAM_START, ReadEventType(event.Create), cryptKeyProvider, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 1; i <= 5; i++ {
		e := <-stream
		if e.Type != event.Create {
			t.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if e.Data.Id != i {
			t.Error(fmt.Errorf("missmatch event data id"))
			return
		}
		if e.Data.Name != "test" {
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
	id := uuid.Must(uuid.NewV7())
	for i := 0; i < 2; i++ {
		data := dd{
			Id:   i,
			Name: "test",
		}
		meta := md{
			Extra: "extra metadata test",
		}
		e, err := event.NewBuilder[dd]().
			WithId(id).
			WithType(event.Create).
			WithData(data).
			WithMetadata(event.Metadata{
				Extra: map[string]any{"extra": meta.Extra},
			}).
			BuildStore()
		if err != nil {
			t.Error(err)
			return
		}
		_, err = es.Store(e,
			cryptKeyProvider)
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
	stream, err := NewStream[dd](es, []event.Type{event.Create}, store.STREAM_START, ReadEventType(event.Create), cryptKeyProvider, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 1; i <= 5; i++ {
		e := <-stream
		if e.Type != event.Create {
			t.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if e.Data.Id != i {
			t.Error(fmt.Errorf("missmatch event data id"))
			return
		}
		if e.Data.Name != "test" {
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
	log.SetLevel(log.ERROR)
	pers, err := eventstore.Init()
	if err != nil {
		b.Error(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	est, err := Init(pers, fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	stream, err := NewStream[dd](est, []event.Type{event.Create}, store.STREAM_START, ReadEventType(event.Create), cryptKeyProvider, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	events := make([]event.StoreEvent, b.N)
	for i := 0; i < b.N; i++ {
		data := dd{
			Id:   i,
			Name: "test" + b.Name(),
		}
		meta := md{
			Extra: "extra metadata test",
		}
		events[i], err = event.NewBuilder[dd]().
			WithType(event.Create).
			WithData(data).
			WithMetadata(event.Metadata{
				Extra: map[string]any{"extra": meta.Extra},
			}).
			BuildStore()
		if err != nil {
			b.Error(err)
			return
		}
	}
	for i := 0; i < b.N; i++ {
		if i == 0 {
			fmt.Println(events[i].Data)
		}
		_, err = est.Store(events[i],
			cryptKeyProvider)
		if err != nil {
			b.Error(err)
			return
		}
	}
	for i := 0; i < b.N; i++ {
		e := <-stream
		if e.Type != event.Create {
			b.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if e.Id.String() == "" {
			b.Error(fmt.Errorf("missing event id"))
			return
		}
		if e.Data != events[i].Data {
			b.Error(fmt.Errorf("missmatch event data, %v != %v", e.Data, events[i].Data))
			return
		}
	}
}
