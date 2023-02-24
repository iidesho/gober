package websocket

import (
	"context"
	"fmt"
	"github.com/cantara/gober/webserver"
	"github.com/gin-gonic/gin"
	"net/url"
	"testing"
	"time"
)

type TT struct {
	Data string `json:"data"`
}

func TestServe(t *testing.T) {
	serv, err := webserver.Init(4123)
	if err != nil {
		t.Error(err)
		return
	}
	Serve[TT](serv.API, "/wstest", nil, func(reader <-chan TT, writer chan<- Write[TT], params gin.Params, ctx context.Context) {
		errChan := make(chan error, 1)
		writer <- Write[TT]{
			Data: <-reader,
			Err:  errChan,
		}
		err := <-errChan
		if err != nil {
			t.Error(err)
			return
		}
	})
	go serv.Run()
}

var reader <-chan TT
var writer chan<- Write[TT]

func TestDial(t *testing.T) {
	u, err := url.Parse("ws://localhost:4123/wstest")
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(u.String())
	fmt.Println(u.Scheme)
	reader, writer, err = Dial[TT](u, context.Background())
	if err != nil {
		t.Error(err)
		return
	}
}

var data = TT{
	Data: "test data",
}

func TestWrite(t *testing.T) {
	errChan := make(chan error, 1)
	select {
	case writer <- Write[TT]{
		Data: data,
		Err:  errChan,
	}:
	case <-time.Tick(time.Second * 10):
		t.Error("could not write in 10s")
		return
	}

	err := <-errChan
	if err != nil {
		t.Error(err)
		return
	}
}

func TestRead(t *testing.T) {
	read := <-reader
	if read.Data != data.Data {
		t.Error("read data is not the same as wrote data, read ", read, " wrote ", data)
	}
}
