package eventmap

import (
	"context"
	"fmt"

	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/consumer"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/sync"

	"github.com/iidesho/gober/crypto"
	"github.com/iidesho/gober/stream/event/store"
)

type EventMap[BT any, T bcts.ReadWriter[BT]] interface {
	Get(key bcts.TinyString) (data T, err error)
	Exists(key bcts.TinyString) (exists bool)
	Len() (l int)
	Keys() (keys []bcts.TinyString)
	Range(f func(key bcts.TinyString, value T) bool)
	GetMap() map[bcts.TinyString]T
	Delete(key bcts.TinyString) (err error)
	Set(key bcts.TinyString, data T) (err error)
	Stream(
		eventTypes []event.Type,
		from store.StreamPosition,
		filter stream.Filter,
		ctx context.Context,
	) (out <-chan event.Event[T], err error)
}

type mapData[BT any, T bcts.ReadWriter[BT]] struct {
	data             *sync.Map[BT, T]
	eventTypeName    string
	eventTypeVersion string
	provider         stream.CryptoKeyProvider
	es               consumer.Consumer[kv[T]]
}

type kv[DT any] struct {
	Key   string `json:"key"`
	Value DT     `json:"value"`
}

func Init[BT any, T bcts.ReadWriter[BT]](
	pers stream.Stream,
	eventType, dataTypeVersion string,
	p stream.CryptoKeyProvider,
	ctx context.Context,
	opts ...func(event.Type, *T),
) (ed EventMap[BT, T], err error) {
	es, err := consumer.New[kv[T]](pers, p, ctx)
	m := mapData[BT, T]{
		data:             sync.NewMap[BT, T](),
		eventTypeName:    eventType,
		eventTypeVersion: dataTypeVersion,
		provider:         p,
		es:               es,
	}
	if err != nil {
		return
	}
	readChan, err := es.Stream(event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctx)
	if err != nil {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-readChan:
				func() {
					defer e.Acc()
					for _, opt := range opts {
						opt(e.Type, &e.Data.Value)
					}
					if e.Type == event.Deleted {
						m.data.Delete(bcts.TinyString(e.Data.Key))
						return
					}
					m.data.Set(bcts.TinyString(e.Data.Key), e.Data.Value)
				}()
			}
		}
	}()

	ed = &m
	return
}

func (m *mapData[T, DT]) Stream(
	eventTypes []event.Type,
	from store.StreamPosition,
	filter stream.Filter,
	ctx context.Context,
) (out <-chan event.Event[DT], err error) {
	s, err := m.es.Stream(eventTypes, from, filter, ctx)
	if err != nil {
		return
	}
	c := make(chan event.Event[DT])
	go func() {
		for e := range s {
			select {
			case <-ctx.Done():
				return
			case c <- event.Event[DT]{
				Type:     e.Type,
				Data:     e.Event.Data.Value,
				Metadata: e.Metadata,
			}:
				continue
			}
		}
	}()
	out = c
	return
}

var ERROR_KEY_NOT_FOUND = fmt.Errorf("provided key does not exist")

func (m *mapData[T, DT]) Get(key bcts.TinyString) (data DT, err error) {
	ed, ok := m.data.Get(key)
	if !ok {
		err = ERROR_KEY_NOT_FOUND
		return
	}
	return ed, nil
}

func (m *mapData[T, DT]) Len() (l int) {
	return len(m.Keys())
}

func (m *mapData[T, DT]) Keys() (keys []bcts.TinyString) {
	keys = make([]bcts.TinyString, 0)
	m.Range(func(k bcts.TinyString, _ DT) bool {
		keys = append(keys, k)
		return true
	})
	return
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the Map's
// contents: no key will be visited more than once, but if the value for any key
// is stored or deleted concurrently (including by f), Range may reflect any
// mapping for that key from any point during the Range call. Range does not
// block other methods on the receiver; even f itself may call any method on m.
func (m *mapData[T, DT]) Range(f func(key bcts.TinyString, value DT) bool) {
	for k, v := range m.GetMap() {
		if !f(k, v) {
			return
		}
	}
}

func (m *mapData[T, DT]) GetMap() map[bcts.TinyString]DT {
	return m.data.GetMap()
}

func (m *mapData[T, DT]) Exists(key bcts.TinyString) (exists bool) {
	_, exists = m.data.Get(bcts.TinyString(key))
	return
}

func (m *mapData[T, DT]) createEvent(
	key bcts.TinyString,
	data DT,
) (e event.Event[kv[DT]], err error) {
	eventType := event.Created
	if m.Exists(key) {
		eventType = event.Updated
	}

	e = event.Event[kv[DT]]{
		Type: eventType,
		Data: kv[DT]{
			Key:   string(key),
			Value: data,
		},
		Metadata: event.Metadata{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Key:      crypto.SimpleHash(string(key)),
		},
	}
	return
}

func (m *mapData[T, DT]) Delete(key bcts.TinyString) (err error) {
	e := event.Event[kv[DT]]{
		Type: event.Deleted,
		Data: kv[DT]{
			Key: string(key),
		},
		Metadata: event.Metadata{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Key:      crypto.SimpleHash(string(key)),
		},
	}
	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	return
}

func (m *mapData[T, DT]) Set(key bcts.TinyString, data DT) (err error) {
	log.Trace("Set and wait start", "key", key)
	e, err := m.createEvent(key, data)
	if err != nil {
		return
	}
	we := event.NewWriteEvent(e)
	m.es.Write() <- we
	<-we.Done() //Missing error
	log.Trace("Set and wait end", "key", key)
	return
}
