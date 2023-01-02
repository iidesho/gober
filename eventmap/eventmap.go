package eventmap

import (
	"context"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/stream"
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

type transactionCheck struct {
	transaction  uint64
	completeChan chan struct{}
}

type mapData[DT any] struct {
	data             sync.Map
	transactionChan  chan transactionCheck
	eventTypeName    string
	eventTypeVersion string
	provider         stream.CryptoKeyProvider
	es               stream.Stream
	esh              stream.SetHelper
}

type kv[DT any] struct {
	Key   string `json:"key"`
	Value DT     `json:"value"`
}

func Init[DT any](pers stream.Persistence, eventType, dataTypeVersion, streamName string, p stream.CryptoKeyProvider, ctx context.Context) (ed EventMap[DT], err error) {
	es, err := stream.Init(pers, streamName, ctx)
	if err != nil {
		return
	}
	m := mapData[DT]{
		data:             sync.Map{},
		transactionChan:  make(chan transactionCheck),
		eventTypeName:    eventType,
		eventTypeVersion: dataTypeVersion,
		provider:         p,
		es:               es,
	}
	eventChan, err := stream.NewStream[kv[DT]](es, event.AllTypes(), store.STREAM_START, stream.ReadAll(), m.provider, ctx)
	if err != nil {
		return
	}
	m.esh = stream.InitSetHelper(func(e event.Event[kv[DT]]) {
		m.data.Store(e.Data.Key, e.Data.Value)
	}, func(e event.Event[kv[DT]]) {
		m.data.Delete(e.Data.Key)
	}, m.es, p, eventChan, ctx)

	ed = &m
	return
}

var ERROR_KEY_NOT_FOUND = fmt.Errorf("Provided key does not exist")

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

func (m *mapData[DT]) createEvent(key string, data DT) (e event.StoreEvent, err error) {
	eventType := event.Create
	if m.Exists(key) {
		eventType = event.Update
	}

	return event.NewBuilder[kv[DT]]().
		WithType(eventType).
		WithData(kv[DT]{
			Key:   key,
			Value: data,
		}).
		WithMetadata(event.Metadata{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Key:      crypto.SimpleHash(key),
		}).
		BuildStore()
}

func (m *mapData[DT]) Delete(key string) (err error) {
	e, err := event.NewBuilder[kv[DT]]().
		WithType(event.Delete).
		WithData(kv[DT]{
			Key: key,
		}).
		WithMetadata(event.Metadata{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Key:      crypto.SimpleHash(key),
		}).
		BuildStore()

	err = m.esh.SetAndWait(e)
	return
}

func (m *mapData[DT]) Set(key string, data DT) (err error) {
	log.Println("Set and wait start")
	e, err := m.createEvent(key, data)
	if err != nil {
		return
	}
	err = m.esh.SetAndWait(e)
	log.Println("Set and wait end")
	return
}
