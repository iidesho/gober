//go:build goexperiment.rangefunc

package persistenteventmapttl

import (
	"context"
	"iter"

	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
)

type EventMap[DT any] interface {
	Get(key string) (data DT, err error)
	Exists(key string) (exists bool)
	//Len() (l int)
	//Keys() (keys []string)
	//Range(f func(key string, data DT) error)
	Delete(data DT) (err error)
	Set(data DT) (err error)
	Stream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out <-chan event.Event[DT], err error)
	Range() iter.Seq2[string, DT]
}

func (m *mapData[DT]) Range() iter.Seq2[string, DT] {
	return m.data.Range()
}
