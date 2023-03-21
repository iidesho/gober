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

var gt *testing.T

func TestServe(t *testing.T) {
	serv, err := webserver.Init(4123, true)
	if err != nil {
		t.Error(err)
		return
	}
	gt = t
	Serve[TT](serv.API, "/wstest", nil, func(reader <-chan TT, writer chan<- Write[TT], params gin.Params, ctx context.Context) {
		errChan := make(chan error, 1)
		writer <- Write[TT]{
			Data: <-reader,
			Err:  errChan,
		}
		err := <-errChan
		if err != nil {
			gt.Error(err)
			return
		}
		<-reader
	})
	go serv.Run()
}

var reader <-chan TT
var writer chan<- Write[TT]
var cancel context.CancelFunc

func TestDial(t *testing.T) {
	gt = t
	u, err := url.Parse("ws://localhost:4123/wstest")
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(u.String())
	fmt.Println(u.Scheme)
	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	reader, writer, err = Dial[TT](u, ctx)
	if err != nil {
		t.Error(err)
		return
	}
}

var data = TT{
	Data: "test data",
}

func TestWrite(t *testing.T) {
	gt = t
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
	gt = t
	read := <-reader
	if read.Data != data.Data {
		t.Error("read data is not the same as wrote data, read ", read, " wrote ", data)
	}
}

/*
func TestClose(t *testing.T) {
	gt = t
	<-reader
	<-reader
	<-reader
	<-reader
}
*/
