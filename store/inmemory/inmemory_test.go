package inmemory

import (
	"context"
	"encoding/json"
	"fmt"
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
	for i := 0; i < 10; i++ {
		e := <-stream
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
