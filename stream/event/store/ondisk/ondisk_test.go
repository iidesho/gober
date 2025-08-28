package ondisk_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/iidesho/gober/stream/event/store/ondisk"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigDefault

var (
	es     *ondisk.Stream
	ctx    context.Context
	cancel context.CancelFunc
)

var (
	STREAM_NAME = "TestStoreAndStream_" + uuid.Must(uuid.NewV7()).String()
	position    = store.STREAM_START
)

func TestPreInit(t *testing.T) {
	os.RemoveAll("streams")
}

func TestInit(t *testing.T) {
	t.Log("init test started")
	/*
		dl, _ := log.NewDebugLogger()
		dl.SetDefault()
	*/
	ctx, cancel = context.WithCancel(context.Background())
	var err error
	t.Log("init test init started")
	es, err = ondisk.Init(STREAM_NAME, ctx)
	t.Log("init test init ended")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("init test ended")
}

func TestStore(t *testing.T) {
	t.Log("store test started")
	data := make(map[string]interface{})
	data["id"] = 1
	data["name"] = "test"
	data["data"] = make([]byte, ondisk.MB*6)

	bytes, err := json.Marshal(data)
	if err != nil {
		t.Error(err)
		return
	}
	status := make(chan store.WriteStatus, 1)
	t.Log("store test write started")
	es.Write() <- store.WriteEvent{
		Event: store.Event{
			Id:       uuid.Must(uuid.NewV7()),
			Type:     string(event.Created),
			Data:     bytes,
			Metadata: bytes,
		},
		Status: status,
	}
	t.Log("store test write ended")
	t.Log("store test status read started")
	s := <-status
	t.Log("store test status read ended")
	if s.Error != nil {
		t.Error(s.Error)
		return
	}
	if s.Time.After(time.Now()) {
		t.Error("write time was after current time")
		return
	}
	if s.Position == 0 {
		t.Error("cannot write at position 0")
		return
	}
	t.Log("store test ended")
}

func TestStream(t *testing.T) {
	t.Log("stream test started")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Log("stream test create stream started")
	s, err := es.Stream(position, ctx)
	t.Log("stream test create stream ended")
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("stream test stream read started")
	e := <-s
	t.Log("stream test stream read ended")
	position = store.StreamPosition(e.Position)
	if e.Type != string(event.Created) {
		t.Error(fmt.Errorf("missmatch inMemEvent types"))
		return
	}
	if e.Id.String() == "" {
		t.Error(fmt.Errorf("missing inMemEvent id"))
		return
	}
	data := make(map[string]interface{})
	err = json.Unmarshal(e.Data, &data)
	if err != nil {
		t.Error(err)
		return
	}
	if data["name"] != "test" {
		t.Error("data name is wrong")
		return
	}
	meta := make(map[string]interface{})
	err = json.Unmarshal(e.Metadata, &meta)
	if err != nil {
		t.Error(err)
		return
	}
	if meta["name"] != "test" {
		t.Error("data name is wrong")
		return
	}

	t.Log("stream test ended")
}

func TestStoreMultiple(t *testing.T) {
	data := make(map[string]interface{})

	for i := 2; i < 12; i++ {
		data["id"] = i
		data["name"] = "test"

		bytes, err := json.Marshal(data)
		if err != nil {
			t.Error(err)
			return
		}
		status := make(chan store.WriteStatus, 1)
		es.Write() <- store.WriteEvent{
			Event: store.Event{
				Id:       uuid.Must(uuid.NewV7()),
				Type:     string(event.Created),
				Data:     bytes,
				Metadata: bytes,
			},
			Status: status,
		}
		s := <-status
		if s.Error != nil {
			t.Error(s.Error)
			return
		}
		if s.Time.After(time.Now()) {
			t.Error("write time was after current time")
			return
		}
		if s.Position == 0 {
			t.Error("cannot write at position 0")
			return
		}
	}
}

func TestStreamMultiple(t *testing.T) {
	s, err := es.Stream(position, ctx)
	if err != nil {
		t.Error(err)
		return
	}
	prevPos := uint64(position)
	for i := 0; i < 10; i++ {
		t.Log("reading stream multiple", "i", i)
		e := <-s
		position = store.StreamPosition(e.Position)
		t.Log("position", "pos", position)
		if prevPos < e.Position {
			prevPos = e.Position
		} else {
			t.Errorf("previous transaction id was bigger than current position id. %d >= %d", prevPos, e.Position)
		}
		fmt.Println(e)
		if e.Type != string(event.Created) {
			t.Error(fmt.Errorf("missmatch inMemEvent types"))
			return
		}
		if e.Id.String() == "" {
			t.Error(fmt.Errorf("missing inMemEvent id"))
			return
		}
	}
}

func TestStoreAndStream(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s, err := es.Stream(position, ctx)
		if err != nil {
			t.Error(err)
			return
		}
		<-s
	}(&wg)
	data := make(map[string]interface{})
	data["id"] = 1
	data["name"] = "test"
	data["data"] = make([]byte, ondisk.MB*6)

	bytes, err := json.Marshal(data)
	if err != nil {
		t.Error(err)
		return
	}
	status := make(chan store.WriteStatus, 1)
	es.Write() <- store.WriteEvent{
		Event: store.Event{
			Id:   uuid.Must(uuid.NewV7()),
			Type: string(event.Created),
			Data: bytes,
		},
		Status: status,
	}
	<-status
	wg.Wait()
}

func TestTeardown(t *testing.T) {
	cancel()
}

func BenchmarkStoreAndStream(b *testing.B) {
	// log.SetLevel(log.ERROR) TODO: should add to sbragi
	log.Debug("benchmark start", "b.N ", b.N)
	data := make(map[string]interface{})
	data["id"] = 1
	data["name"] = "test"

	bytes, err := json.Marshal(data)
	if err != nil {
		b.Error(err)
		return
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	es, err = ondisk.Init(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	stream, err := es.Stream(store.STREAM_START, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	for i := 0; i < b.N; i++ {
		status := make(chan store.WriteStatus, 1)
		es.Write() <- store.WriteEvent{
			Event: store.Event{
				Id:   uuid.Must(uuid.NewV7()),
				Type: string(event.Created),
				Data: bytes,
			},
			Status: status,
		}
		s := <-status
		if s.Error != nil {
			b.Error(s.Error)
			return
		}
		if s.Time.After(time.Now()) {
			b.Error("write time was after current time")
			return
		}
		if s.Position == 0 {
			b.Error("cannot write at position 0")
			return
		}
	}
	for i := 0; i < b.N; i++ {
		e := <-stream
		if e.Type != string(event.Created) {
			b.Error(fmt.Errorf("missmatch inMemEvent types"))
			return
		}
		if e.Id.String() == "" { // This is wrong, Not checking anything
			b.Error(fmt.Errorf("missing inMemEvent id"))
			return
		}
	}
}
