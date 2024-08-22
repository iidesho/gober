package stream

import (
	"context"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
)

type Stream interface {
	Write() chan<- store.WriteEvent
	Stream(from store.StreamPosition, ctx context.Context) (out <-chan store.ReadEvent, err error)
	End() (pos uint64, err error)
	Name() string
}

type FilteredStream[BT any, T bcts.ReadWriter[BT]] interface {
	Write() chan<- event.WriteEventReadStatus[BT, T]
	Store(event event.Event[BT, T]) (position uint64, err error)
	Stream(
		eventTypes []event.Type,
		from store.StreamPosition,
		filter Filter,
		ctx context.Context,
	) (out <-chan event.ReadEvent[BT, T], err error)
	End() (pos uint64, err error)
	Name() string
	FilteredEnd(eventTypes []event.Type, filter Filter) (pos uint64, err error)
}

//FilteredStream(eventTypes []event.Type, from store.StreamPosition, filter Filter[MT], cryptKey CryptoKeyProvider, ctx context.Context) (out <-chan event.Event[DT, any], err error)
