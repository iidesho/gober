package consumer_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/consumer"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/iidesho/gober/stream/event/store/inmemory"
	"github.com/iidesho/gober/stream/event/store/ondisk"

	"github.com/gofrs/uuid"
)

var (
	c               consumer.Consumer[dd]
	ctxGlobal       context.Context
	ctxGlobalCancel context.CancelFunc
	testCryptKey    = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
	events          = make(map[int]event.ReadEvent[dd])
)

var STREAM_NAME = "TestConsumer_" + uuid.Must(uuid.NewV7()).String()

type md struct {
	Extra bcts.SmallBytes `json:"extra"`
}

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestInit(t *testing.T) {
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	pers, err := ondisk.Init(STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	c, err = consumer.New[dd](pers, cryptKeyProvider, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestStoreOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(ctxGlobal)
	defer cancel()
	readEventStream, err := c.Stream(event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctx)
	if err != nil {
		t.Error(err)
		return
	}
	for i := 1; i <= 5; i++ {
		data := dd{
			Id:   i,
			Name: "test",
		}
		meta := md{
			Extra: bcts.SmallBytes("extra metadata test"),
		}
		e := event.Event[dd]{
			Type: event.Created,
			Data: data,
			Metadata: event.Metadata{
				Extra: map[bcts.TinyString]bcts.SmallBytes{"extra": meta.Extra},
			},
		}
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			read := <-readEventStream
			events[read.Data.Id] = read.ReadEvent
			read.Acc()
		}()
		we := event.NewWriteEvent(e)
		c.Write() <- we
		status := <-we.Done()
		if status.Error != nil {
			t.Error(status.Error)
			return
		}
		wg.Wait()
	}
	return
}

func TestStreamOrder(t *testing.T) {
	for i := 1; i <= 5; i++ {
		e := events[i]
		if e.Type != event.Created {
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
		if string(e.Metadata.Extra["extra"]) != "extra metadata test" {
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
	// log.SetLevel(log.ERROR) TODO: should add to sbragi
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pers, err := inmemory.Init(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	c, err := consumer.New[[]byte](pers, cryptKeyProvider, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	readEventStream, err := c.Stream(event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	events := make([]event.WriteEventReadStatus[[]byte], b.N)
	go func() {
		for i := 0; i < b.N; i++ {
			e := <-readEventStream
			e.Acc()
			if e.Type != event.Created {
				b.Error(fmt.Errorf("missmatch event types"))
				return
			}
			/*
				if e.Data.Id != events[i].Event().Data.Id {
					b.Error(fmt.Errorf("missmatch event data, %v != %v", e.Data, events[i].Event().Data))
					return
				}
			*/
		}
	}()
	writeEventStream := c.Write()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		we := event.NewWriteEvent(event.Event[[]byte]{
			Type: event.Created,
			Data: make([]byte, 1024),
			Metadata: event.Metadata{
				Extra: map[bcts.TinyString]bcts.SmallBytes{
					"extra": bcts.SmallBytes("extra metadata test"),
				},
			},
		})
		events[i] = we
		writeEventStream <- events[i]
		status := <-events[i].Done()
		if status.Error != nil {
			b.Error(status.Error)
			return
		}
	}
}
