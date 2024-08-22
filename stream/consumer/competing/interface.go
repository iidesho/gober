package competing

import (
	"context"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream/event"
)

type Consumer[BT any, T bcts.ReadWriter[BT]] interface {
	Write() chan<- event.WriteEventReadStatus[BT, T]
	Stream() <-chan ReadEventWAcc[BT, T]
	Completed() <-chan event.ReadEvent[BT, T]
	End() (pos uint64, err error)
	Name() string
}

type ReadEventWAcc[BT any, T bcts.ReadWriter[BT]] struct {
	CTX context.Context
	Acc func(T)
	event.ReadEvent[BT, T]
}
