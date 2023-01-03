package inmemory

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/cantara/bragi"
	"testing"

	"github.com/cantara/gober/store"
	"github.com/gofrs/uuid"
)

var es *EventStore

var STREAM_NAME = "TestStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

func TestInit(t *testing.T) {
	var err error
	es, err = Init()
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestStore(t *testing.T) {
	data := make(map[string]interface{})
	data["id"] = 1
	data["name"] = "test"

	bytes, err := json.Marshal(data)
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = es.Store(STREAM_NAME, ctx, store.Event{
		Id:   uuid.Must(uuid.NewV7()),
		Type: "test",
		Data: bytes,
	})
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := es.Stream(STREAM_NAME, store.STREAM_START, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	e := <-stream
	if e.Type != "test" {
		t.Error(fmt.Errorf("missmatch event types"))
		return
	}
	if e.Id.String() == "" {
		t.Error(fmt.Errorf("missing event id"))
		return
	}
	return
}

func TestStoreMultiple(t *testing.T) {
	data := make(map[string]interface{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 2; i < 11; i++ {
		data["id"] = i
		data["name"] = "test"

		bytes, err := json.Marshal(data)
		if err != nil {
			t.Error(err)
			return
		}
		_, err = es.Store(STREAM_NAME, ctx, store.Event{
			Id:   uuid.Must(uuid.NewV7()),
			Type: "test",
			Data: bytes,
		})
		if err != nil {
			t.Error(err)
			return
		}
	}
	return
}

func TestStreamMultiple(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := es.Stream(STREAM_NAME, store.STREAM_START, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	var position uint64
	for i := 0; i < 10; i++ {
		e := <-stream
		if position < e.Position {
			position = e.Position
		} else {
			t.Errorf("previous transaction id was bigger than current position id. %d >= %d", position, e.Position)
		}
		fmt.Println(e)
		if e.Type != "test" {
			t.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if e.Id.String() == "" {
			t.Error(fmt.Errorf("missing event id"))
			return
		}
	}
	return
}

func BenchmarkStoreAndStream(b *testing.B) { //FIXME: Storing and reading a lot in succession is broken.
	log.SetLevel(log.ERROR)
	log.Debug("b.N ", b.N)
	data := make(map[string]interface{})
	data["id"] = 1
	data["name"] = "test"

	bytes, err := json.Marshal(data)
	if err != nil {
		b.Error(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := Init()
	if err != nil {
		b.Error(err)
		return
	}
	stream, err := s.Stream(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), store.STREAM_START, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		_, err = s.Store(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx, store.Event{
			Id:   uuid.Must(uuid.NewV7()),
			Type: "test",
			Data: bytes,
		})
		if err != nil {
			b.Error(err)
			return
		}
	}
	for i := 0; i < b.N; i++ {
		e := <-stream
		if e.Type != "test" {
			b.Error(fmt.Errorf("missmatch event types"))
			return
		}
		if e.Id.String() == "" {
			b.Error(fmt.Errorf("missing event id"))
			return
		}
	}
}
