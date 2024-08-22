package consumer

import (
	"context"

	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
)

type Consumer[BT any, T bcts.ReadWriter[BT]] interface {
	Write() chan<- event.WriteEventReadStatus[BT, T]
	Stream(
		eventTypes []event.Type,
		from store.StreamPosition,
		filter stream.Filter,
		ctx context.Context,
	) (out <-chan event.ReadEventWAcc[BT, T], err error)
	Name() string
	End() (pos uint64, err error)
	FilteredEnd(eventTypes []event.Type, filter stream.Filter) (pos uint64, err error)
}
