package eventmap

import (
	"context"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/event"
	"sync"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/store"
)

type EventMap[DT any] interface {
	Get(key string) (data DT, err error)
	Exists(key string) (exists bool)
	Len() (l int)
	Keys() (keys []string)
	Range(f func(key, value any) bool)
	Delete(key string) (err error)
	Set(key string, data DT) (err error)
}

type mapData[DT any] struct {
	data             sync.Map
	eventTypeName    string
	eventTypeVersion string
	provider         stream.CryptoKeyProvider
	es               consumer.Consumer[kv[DT]]
}

type kv[DT any] struct {
	Key   string `json:"key"`
	Value DT     `json:"value"`
}

func Init[DT any](pers stream.Persistence, eventType, dataTypeVersion, streamName string, p stream.CryptoKeyProvider, ctx context.Context) (ed EventMap[DT], err error) {
	s, err := stream.Init(pers, streamName, ctx)
	if err != nil {
		return
	}
	es, eventChan, err := consumer.New[kv[DT]](s, p, event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctx)
	m := mapData[DT]{
		data:             sync.Map{},
		eventTypeName:    eventType,
		eventTypeVersion: dataTypeVersion,
		provider:         p,
		es:               es,
	}
	if err != nil {
		return
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-eventChan:
				func() {
					defer e.Acc()
					if e.Type == event.Delete {
						m.data.Delete(e.Data.Key)
						return
					}
					m.data.Store(e.Data.Key, e.Data.Value)
				}()
			}
		}
	}()

	ed = &m
	return
}

var ERROR_KEY_NOT_FOUND = fmt.Errorf("provided key does not exist")

func (m *mapData[DT]) Get(key string) (data DT, err error) {
	ed, ok := m.data.Load(key)
	if !ok {
		err = ERROR_KEY_NOT_FOUND
		return
	}
	return ed.(DT), nil
}

func (m *mapData[DT]) Len() (l int) {
	return len(m.Keys())
}

func (m *mapData[DT]) Keys() (keys []string) {
	keys = make([]string, 0)
	m.data.Range(func(k, _ any) bool {
		keys = append(keys, k.(string))
		return true
	})
	return
}

func (m *mapData[DT]) Range(f func(key, value any) bool) {
	m.data.Range(f)
}

func (m *mapData[DT]) Exists(key string) (exists bool) {
	_, exists = m.data.Load(key)
	return
}

func (m *mapData[DT]) createEvent(key string, data DT) (e consumer.Event[kv[DT]], err error) {
	eventType := event.Create
	if m.Exists(key) {
		eventType = event.Update
	}

	e = consumer.Event[kv[DT]]{
		Type: eventType,
		Data: kv[DT]{
			Key:   key,
			Value: data,
		},
		Metadata: event.Metadata{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Key:      crypto.SimpleHash(key),
		},
	}
	return
}

func (m *mapData[DT]) Delete(key string) (err error) {
	e := consumer.Event[kv[DT]]{
		Type: event.Delete,
		Data: kv[DT]{
			Key: key,
		},
		Metadata: event.Metadata{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Key:      crypto.SimpleHash(key),
		},
	}

	_, err = m.es.Store(e)
	return
}

func (m *mapData[DT]) Set(key string, data DT) (err error) {
	log.Println("Set and wait start")
	e, err := m.createEvent(key, data)
	if err != nil {
		return
	}
	_, err = m.es.Store(e)
	log.Println("Set and wait end")
	return
}
