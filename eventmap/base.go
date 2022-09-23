package eventmap

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/cantara/bragi"

	event "github.com/cantara/gober"
	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/store"
	"github.com/google/uuid"
)

type EventMap[DT, MT any] interface {
	Get(key string) (data DT, metadata event.Metadata[MT], err error)
	Exists(key string) (exists bool)
	Len() (l int)
	Keys() (keys []string)
	Range(f func(key, value any) bool)
	Delete(key string) (err error)
	Set(key string, data DT, metadata MT) (err error)
	Close()
}

type transactionCheck struct {
	transaction  uint64
	completeChan chan struct{}
}

type mapData[DT, MT any] struct {
	data             sync.Map
	transactionChan  chan transactionCheck
	eventTypeName    string
	eventTypeVersion string
	createEventType  string
	setEventType     string
	deleteEventType  string
	provider         event.CryptoKeyProvider
	es               event.EventService[kv[DT], MT]
	closeES          func()
}

type kv[DT any] struct {
	Key   string `json:"key"`
	Value DT     `json:"value"`
}

type dmd[DT, MT any] struct {
	Data     DT
	Metadata event.Metadata[MT]
}

func Init[DT, MT any](pers event.Persistence, eventType, dataTypeVersion, stream string, p event.CryptoKeyProvider, ctx context.Context) (ed *mapData[DT, MT], err error) {
	ctxES, cancel := context.WithCancel(ctx)
	es, err := event.Init[kv[DT], MT](pers, stream, ctxES)
	if err != nil {
		return
	}
	ed = &mapData[DT, MT]{
		data:             sync.Map{},
		transactionChan:  make(chan transactionCheck),
		eventTypeName:    eventType,
		eventTypeVersion: dataTypeVersion,
		createEventType:  fmt.Sprintf("create_%s", eventType),
		setEventType:     fmt.Sprintf("set_%s", eventType),
		deleteEventType:  fmt.Sprintf("delete_%s", eventType),
		provider:         p,
		es:               es,
		closeES:          cancel,
	}
	eventTypes := []string{ed.createEventType, ed.setEventType, ed.deleteEventType}
	eventChan, err := ed.es.Stream(eventTypes, store.STREAM_START, event.ReadAll[MT](), ed.provider, ctxES)
	if err != nil {
		return
	}
	upToDate := false
	for !upToDate {
		time.Sleep(time.Millisecond)
		select {
		case <-ctx.Done():
			return
		case event := <-eventChan:
			if ed.deleteEventType != "" && event.Type == ed.deleteEventType {
				ed.data.Delete(event.Data.Key)
				continue
			}
			ed.data.Store(event.Data.Key, dmd[DT, MT]{
				Data:     event.Data.Value,
				Metadata: event.Metadata,
			})
		default:
			upToDate = true
		}
	}

	transactionChan := make(chan uint64, 5)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-eventChan:
				//log.Println("event got", string(event.Data.Key))
				if ed.deleteEventType != "" && event.Type == ed.deleteEventType {
					ed.data.Delete(event.Data.Key)
					transactionChan <- event.Transaction
					continue
				}
				ed.data.Store(event.Data.Key, dmd[DT, MT]{
					Data:     event.Data.Value,
					Metadata: event.Metadata,
				})
				transactionChan <- event.Transaction
				//log.Printf("stored (%s) %s", d.Key, d.Data)
			}
		}
	}()

	go func() {
		completeChans := make(map[string]transactionCheck)
		var currentTransaction uint64
		for {
			select {
			case <-ctx.Done():
				return
			case completeChan := <-ed.transactionChan:
				log.Println("Check pre: ", completeChan.transaction, currentTransaction)
				if currentTransaction >= completeChan.transaction {
					completeChan.completeChan <- struct{}{}
					continue
				}
				completeChans[uuid.New().String()] = completeChan
			case transaction := <-transactionChan:
				log.Println("Check ins: ", transaction, currentTransaction)
				if currentTransaction < transaction {
					currentTransaction = transaction
				}
				for id, completeChan := range completeChans {
					log.Println("Check deep: ", completeChan.transaction, transaction)
					if transaction < completeChan.transaction {
						continue
					}
					completeChan.completeChan <- struct{}{}
					delete(completeChans, id)
				}
			}
		}
	}()
	return
}

var ERROR_KEY_NOT_FOUND = fmt.Errorf("Provided key does not exist")

func (m mapData[DT, MT]) Get(key string) (data DT, metadata event.Metadata[MT], err error) {
	ed, ok := m.data.Load(key)
	if !ok {
		err = ERROR_KEY_NOT_FOUND
		return
	}
	tmp := ed.(dmd[DT, MT])
	return tmp.Data, tmp.Metadata, nil
}

func (m mapData[DT, MT]) Len() (l int) {
	return len(m.Keys())
}

func (m mapData[DT, MT]) Keys() (keys []string) {
	keys = make([]string, 0)
	m.data.Range(func(k, _ any) bool {
		keys = append(keys, k.(string))
		return true
	})
	return
}

func (m mapData[DT, MT]) Range(f func(key, value any) bool) {
	m.data.Range(f)
}

func (m *mapData[DT, MT]) Exists(key string) (exists bool) {
	_, exists = m.data.Load(key)
	return
}

func (m mapData[DT, MT]) createEvent(key string, data DT, metadata MT) (e event.Event[kv[DT], MT], err error) {
	eventType := m.createEventType
	if m.Exists(key) {
		eventType = m.setEventType
	}

	return event.EventBuilder[kv[DT], MT]().
		WithType(eventType).
		WithData(kv[DT]{
			Key:   key,
			Value: data,
		}).
		WithMetadata(event.Metadata[MT]{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Event:    metadata,
			Key:      crypto.SimpleHash(key),
		}).
		Build()
}

func (m *mapData[DT, MT]) Delete(key string) (err error) {
	e, err := event.EventBuilder[kv[DT], MT]().
		WithType(m.deleteEventType).
		WithData(kv[DT]{
			Key: key,
		}).
		WithMetadata(event.Metadata[MT]{
			Version:  m.eventTypeVersion,
			DataType: m.eventTypeName,
			Key:      crypto.SimpleHash(key),
		}).
		Build()

	err = m.setAndWait(e)
	return
}

func (m *mapData[DT, MT]) setAndWait(e event.Event[kv[DT], MT]) (err error) {
	transaction, err := m.es.Store(e, m.provider)
	if err != nil {
		return
	}
	completeChan := make(chan struct{})
	defer close(completeChan)
	m.transactionChan <- transactionCheck{
		transaction:  transaction,
		completeChan: completeChan,
	}
	log.Println("Set and wait waiting")
	<-completeChan
	return
}

func (m *mapData[DT, MT]) Set(key string, data DT, metadata MT) (err error) {
	log.Println("Set and wait start")
	e, err := m.createEvent(key, data, metadata)
	if err != nil {
		return
	}
	err = m.setAndWait(e)
	log.Println("Set and wait end")
	return
}

func (m mapData[DT, MT]) Close() {
	m.closeES()
}
