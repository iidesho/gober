package competing

import (
	"context"

	"github.com/cantara/gober/stream/event"
)

type Consumer[T any] interface {
	Write() chan<- event.WriteEventReadStatus[T]
	Stream() <-chan ReadEventWAcc[T]
	Completed() <-chan event.ReadEvent[T]
	End() (pos uint64, err error)
	Name() string
}

type ReadEventWAcc[T any] struct {
	event.ReadEvent[T]

	Acc func(T)
	CTX context.Context
}
