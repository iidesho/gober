package consumer

import (
	"context"
	"fmt"
	"github.com/cantara/gober/store/eventstore"
	"github.com/cantara/gober/stream"
	"sync"
	"testing"

	"github.com/gofrs/uuid"

	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream/event"
)

var c Consumer[dd]
var eventStream <-chan ReadEvent[dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
var events = make(map[int]ReadEvent[dd])

var STREAM_NAME = "TestConsumer_" + uuid.Must(uuid.NewV7()).String()

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
	pers, err := eventstore.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	est, err := stream.Init(pers, STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	c, eventStream, err = New[dd](est, cryptKeyProvider, event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
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
		e := Event[dd]{
			Type: event.Create,
			Data: data,
			Metadata: event.Metadata{
				Extra: map[string]any{"extra": meta.Extra},
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			read := <-eventStream
			events[read.Data.Id] = read
			read.Acc()
		}()
		_, err := c.Store(e)
		if err != nil {
			t.Error(err)
			return
		}
		wg.Wait()
	}
	return
}

func TestStreamOrder(t *testing.T) {
	for i := 1; i <= 5; i++ {
		e := events[i]
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
