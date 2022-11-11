package stream

import (
	"context"
	"encoding/json"
	"time"

	log "github.com/cantara/bragi"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream/event"
)

type eventService[DT, MT any] struct {
	store      Persistence
	streamName string
	writeChan  chan eventWrite
}

type eventWrite struct {
	event      store.Event
	returnChan chan eventWriteReturn
}

type eventWriteReturn struct {
	transactionId uint64
	err           error
}

type Filter[MT any] func(md event.Metadata[MT]) bool

type CryptoKeyProvider func(key string) string

func ReadAll[MT any]() Filter[MT] {
	return func(_ event.Metadata[MT]) bool { return false }
}

func ReadEventType[MT any](t event.Type) Filter[MT] {
	return func(md event.Metadata[MT]) bool { return md.EventType != t }
}

func ReadDataType[MT any](t string) Filter[MT] {
	return func(md event.Metadata[MT]) bool { return md.DataType != t }
}

const BATCH_SIZE = 5000 //5000 is an arbitrary number, should probably be based on something else.

func Init[DT, MT any](st Persistence, stream string, ctx context.Context) (out Stream[DT, MT], err error) {
	es := eventService[DT, MT]{
		store:      st,
		streamName: stream,
		writeChan:  make(chan eventWrite, BATCH_SIZE),
	}
	out = es
	go func() {
		var events []store.Event
		var returnChans []chan eventWriteReturn
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-es.writeChan:
				events = append(events, e.event)
				returnChans = append(returnChans, e.returnChan)
				for {
					done := false
					select {
					case <-ctx.Done():
						return
					case e = <-es.writeChan:
						events = append(events, e.event)
						returnChans = append(returnChans, e.returnChan)
					default:
						done = true
					}
					if done || len(events) >= BATCH_SIZE {
						break
					}
				}

				transactionId, err := es.store.Store(es.streamName, ctx, events...)
				for _, returnChan := range returnChans {
					returnChan <- eventWriteReturn{
						transactionId: transactionId,
						err:           err,
					}
				}
				events = nil
				returnChans = nil
			}
		}
	}()
	return
}

func (es eventService[DT, MT]) Store(e event.Event[DT, MT], cryptoKey CryptoKeyProvider) (transactionId uint64, err error) {
	if e.Type == "" {
		err = event.MissingTypeError
		return
	}
	e.Metadata.Stream = es.streamName
	e.Metadata.EventType = e.Type
	e.Metadata.Created = time.Now()
	metadataByte, err := json.Marshal(e.Metadata)
	if err != nil {
		return
	}

	data, err := json.Marshal(e.Data)
	if err != nil {
		return
	}
	edata, err := crypto.Encrypt(data, cryptoKey(e.Metadata.Key))
	if err != nil {
		return
	}

	eventData := store.Event{
		Id:       e.Id,
		Type:     e.Type,
		Data:     edata,
		Metadata: metadataByte,
	}

	//Go sync pattern
	returnChan := make(chan eventWriteReturn, 1)
	defer close(returnChan)
	es.writeChan <- eventWrite{
		event:      eventData,
		returnChan: returnChan,
	}
	writeReturn := <-returnChan

	return writeReturn.transactionId, writeReturn.err
}

func (es eventService[DT, MT]) Stream(eventTypes []event.Type, from store.StreamPosition, filter Filter[MT], cryptKey CryptoKeyProvider, ctx context.Context) (out <-chan event.Event[DT, MT], err error) {
	filterEventTypes := len(eventTypes) > 0
	ets := make(map[event.Type]struct{})
	for _, eventType := range eventTypes {
		ets[eventType] = struct{}{}
	}
	stream, err := es.store.Stream(es.streamName, from, ctx)
	if err != nil {
		return
	}
	eventChan := make(chan event.Event[DT, MT], BATCH_SIZE)
	out = eventChan
	go func() {
		defer close(eventChan)
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-stream:
				if filterEventTypes {
					if _, ok := ets[e.Type]; !ok {
						continue
					}
				}
				var metadata event.Metadata[MT]
				err := json.Unmarshal(e.Metadata, &metadata)
				if err != nil {
					log.AddError(err).Warning("Unmarshaling event metadata error")
					continue
				}
				if filter(metadata) {
					continue
				}
				dataJson, err := crypto.Decrypt(e.Data, cryptKey(metadata.Key))
				if err != nil {
					log.AddError(err).Warning("Decrypting event data error")
					return
				}
				var data DT
				err = json.Unmarshal(dataJson, &data)
				if err != nil {
					log.AddError(err).Warning("Unmarshaling event data error")
					continue
				}

				log.Debug("Read event: ", e.Position)
				eventChan <- event.Event[DT, MT]{
					Id:       e.Id,
					Type:     e.Type,
					Data:     data,
					Metadata: metadata,

					Transaction: e.Transaction,
					Position:    e.Position,
					Created:     e.Created,
				}
			}
		}
	}()
	return
}

func (s eventService[DT, MT]) Name() string {
	return s.streamName
}
