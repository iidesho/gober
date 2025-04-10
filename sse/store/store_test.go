package store_test

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	sseStore "github.com/iidesho/gober/sse/store"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/iidesho/gober/stream/event/store/inmemory"
	"github.com/iidesho/gober/webserver"
)

var log = sbragi.WithLocalScope(sbragi.LevelError)

var (
	gt     *testing.T
	wg     sync.WaitGroup
	cancel context.CancelFunc
	ctx    context.Context
	ws     stream.Stream
	rs     stream.Stream
)

func TestServe(t *testing.T) {
	serv, err := webserver.Init(4128, true)
	if err != nil {
		t.Error(err)
		return
	}
	gt = t
	ctx, cancel = context.WithCancel(context.Background())
	ws, err = inmemory.Init("sse_test", ctx)
	if err != nil {
		t.Error(err)
		cancel()
		return
	}
	sseStore.CreateEndpoint(serv.Base(), ws, "events")
	go serv.Run()
}

func TestConnect(t *testing.T) {
	gt = t
	u, err := url.Parse("http://localhost:4128/events")
	if err != nil {
		t.Error(err)
		return
	}
	rs, err = sseStore.Connect(http.DefaultClient, u, ctx)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestWrite(t *testing.T) {
	gt = t
	select {
	case ws.Write() <- store.WriteEvent{
		Event: store.Event{
			Type: "test",
			Data: []byte("some test data"),
			Id:   uuid.Must(uuid.NewV7()),
		},
	}:
	case <-time.Tick(time.Second * 10):
		t.Error("could not write in 10s")
		return
	}
}

func TestRead(t *testing.T) {
	gt = t
	reader, err := rs.Stream(store.STREAM_START, ctx)
	if err != nil {
		t.Fatal(err)
	}
	read := <-reader
	if string(read.Data) != "some test data" {
		t.Error("read data is not the same as wrote data, read ", read, " wrote ", "some test data")
	}
}
