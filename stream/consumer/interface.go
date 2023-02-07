package consumer

import (
	"context"
	"github.com/cantara/gober/stream/event"
)

type Event[T any] struct {
	Type     event.Type
	Data     T
	Metadata event.Metadata
}

type ReadEvent[T any] struct {
	Event[T]

	Position uint64

	Acc func()
	CTX context.Context
}

type Consumer[T any] interface {
	Store(event Event[T]) (position uint64, err error)
	//Stream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter) (out <-chan ReadEvent[T], err error)
	End() (pos uint64, err error)
	Name() string
}
