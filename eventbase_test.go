package gober

import (
	"context"
	"fmt"
	"testing"

	"github.com/cantara/gober/store"
	"github.com/cantara/gober/store/inmemory"
	"github.com/gofrs/uuid"
)

var es EventService[dd, md]
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
	est, err := Init[dd, md](pers, STREAM_NAME, ctxGlobal)
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
		event, err := EventBuilder[dd, md]().
			WithType("test").
			WithData(data).
			WithMetadata(Metadata[md]{
				Event: meta,
			}).
			Build()
		if err != nil {
			t.Error(err)
			return
		}
		_, err = es.Store(event,
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
	stream, err := es.Stream([]string{"test"}, store.STREAM_START, ReadType[md]("test"), cryptKeyProvider, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 1; i <= 5; i++ {
		e := <-stream
		if e.Type != "test" {
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
		if e.Metadata.Event.Extra != "extra metadata test" {
			t.Error(fmt.Errorf("missmatch event metadata extra"))
			return
		}
		if e.Metadata.Type != e.Type {
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
		event, err := EventBuilder[dd, md]().
			WithId(id).
			WithType("test").
			WithData(data).
			WithMetadata(Metadata[md]{
				Event: meta,
			}).
			Build()
		if err != nil {
			t.Error(err)
			return
		}
		_, err = es.Store(event,
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
	stream, err := es.Stream([]string{"test"}, store.STREAM_START, ReadType[md]("test"), cryptKeyProvider, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 1; i <= 5; i++ {
		e := <-stream
		if e.Type != "test" {
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
		if e.Metadata.Event.Extra != "extra metadata test" {
			t.Error(fmt.Errorf("missmatch event metadata extra"))
			return
		}
		if e.Metadata.Type != e.Type {
			t.Error(fmt.Errorf("missmatch event metadata type and event type"))
			return
		}
	}
	return
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}
