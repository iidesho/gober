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
	End() (pos store.StreamPosition, err error)
	Name() string
}

type ShardableStream interface {
	StreamShard(
		shard string,
		from store.StreamPosition,
		ctx context.Context,
	) (out <-chan store.ReadEvent, err error)
	Stream
}

type FilteredStream[BT any, T bcts.ReadWriter[BT]] interface {
	Write() chan<- event.WriteEventReadStatus[BT, T]
	Store(event event.Event[BT, T]) (position store.StreamPosition, err error)
	Stream(
		eventTypes []event.Type,
		from store.StreamPosition,
		filter Filter,
		ctx context.Context,
	) (out <-chan event.ReadEvent[BT, T], err error)
	End() (pos store.StreamPosition, err error)
	Name() string
	FilteredEnd(eventTypes []event.Type, filter Filter) (pos store.StreamPosition, err error)
}

type ShardableFilteredStream[BT any, T bcts.ReadWriter[BT]] interface {
	StreamShard(
		shard string,
		eventTypes []event.Type,
		from store.StreamPosition,
		filter Filter,
		ctx context.Context,
	) (out <-chan event.ReadEvent[BT, T], err error)
	FilteredStream[BT, T]
}

// FilteredStream(eventTypes []event.Type, from store.StreamPosition, filter Filter[MT], cryptKey CryptoKeyProvider, ctx context.Context) (out <-chan event.Event[DT, any], err error)
