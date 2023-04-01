package websocket

import (
	"context"
	"fmt"
	"github.com/cantara/gober/webserver"
	"github.com/gin-gonic/gin"
	"net/url"
	"sync"
	"testing"
	"time"
)

type TT struct {
	Data  string `json:"data"`
	Bytes []byte `json:"bytes"`
}

var gt *testing.T
var wg sync.WaitGroup

func TestServe(t *testing.T) {
	serv, err := webserver.Init(4123, true)
	if err != nil {
		t.Error(err)
		return
	}
	gt = t
	Serve[TT](serv.API, "/wstest", nil, func(reader <-chan TT, writer chan<- Write[TT], params gin.Params, ctx context.Context) {
		defer close(writer)
		wg.Add(1)
		defer wg.Done()
		for read := range reader {
			errChan := make(chan error, 1)
			writer <- Write[TT]{
				Data: read,
				Err:  errChan,
			}
			err := <-errChan
			if err != nil {
				gt.Error(err)
				return
			}
		}
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

func TestWriteAgain(t *testing.T) {
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

func TestReadAgain(t *testing.T) {
	gt = t
	read := <-reader
	if read.Data != data.Data {
		t.Error("read data is not the same as wrote data, read ", read, " wrote ", data)
	}
}

func TestWriteAndReadLarge(t *testing.T) {
	gt = t
	byteLen := 1000 * 1000 * 1000
	errChan := make(chan error, 1)
	select {
	case writer <- Write[TT]{
		Data: TT{
			Data:  "Large",
			Bytes: make([]byte, byteLen),
		},
		Err: errChan,
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

	read := <-reader
	if read.Data != "Large" {
		t.Error("read data is not the same as wrote data, read ", read, " wrote ", data)
	}
	if len(read.Bytes) != byteLen {
		t.Error("read data is not the same as wrote data, read ", read, " wrote ", data)
	}
	close(writer)
	wg.Wait()
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
