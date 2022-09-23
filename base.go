package gober

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cantara/gober/crypto"
	"time"

	log "github.com/cantara/bragi"

	"github.com/cantara/gober/store"
)

type EventService[DT, MT any] struct {
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

type Filter[MT any] func(md Metadata[MT]) bool

type CryptoKeyProvider func(key string) string

var MissingTypeError = fmt.Errorf("Event type is missing")

func ReadAll[MT any]() Filter[MT] {
	return func(_ Metadata[MT]) bool { return false }
}
func ReadType[MT any](t string) Filter[MT] {
	return func(md Metadata[MT]) bool { return md.Type != t }
}

const BATCH_SIZE = 5000 //5000 is an arbitrary number, should probably be based on something else.

func Init[DT, MT any](st Persistence, stream string, ctx context.Context) (es EventService[DT, MT], err error) {
	es = EventService[DT, MT]{
		store:      st,
		streamName: stream,
		writeChan:  make(chan eventWrite, BATCH_SIZE),
	}
	go func() {
		var events []store.Event
		var returnChans []chan eventWriteReturn
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-es.writeChan:
				events = append(events, event.event)
				returnChans = append(returnChans, event.returnChan)
				for {
					done := false
					select {
					case <-ctx.Done():
						return
					case event := <-es.writeChan:
						events = append(events, event.event)
						returnChans = append(returnChans, event.returnChan)
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

func (es EventService[DT, MT]) Store(event Event[DT, MT], cryptoKey CryptoKeyProvider) (transactionId uint64, err error) {
	if event.Type == "" {
		err = MissingTypeError
		return
	}
	event.Metadata.Stream = es.streamName
	event.Metadata.Type = event.Type
	event.Metadata.Created = time.Now()
	metadataByte, err := json.Marshal(event.Metadata)
	if err != nil {
		return
	}

	data, err := json.Marshal(event.Data)
	if err != nil {
		return
	}
	edata, err := crypto.Encrypt(data, cryptoKey(event.Metadata.Key))
	if err != nil {
		return
	}

	eventData := store.Event{
		Id:       event.Id,
		Type:     event.Type,
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

func (es EventService[DT, MT]) Stream(eventTypes []string, from store.StreamPosition, filter Filter[MT], cryptKey CryptoKeyProvider, ctx context.Context) (out <-chan Event[DT, MT], err error) {
	filterEventTypes := len(eventTypes) > 0
	ets := make(map[string]struct{})
	for _, eventType := range eventTypes {
		ets[eventType] = struct{}{}
	}
	stream, err := es.store.Stream(es.streamName, from, ctx)
	if err != nil {
		return
	}
	eventChan := make(chan Event[DT, MT], BATCH_SIZE)
	out = eventChan
	go func() {
		defer close(eventChan)
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-stream:
				if filterEventTypes {
					if _, ok := ets[event.Type]; !ok {
						continue
					}
				}
				var metadata Metadata[MT]
				err := json.Unmarshal(event.Metadata, &metadata)
				if err != nil {
					log.AddError(err).Warning("Unmarshaling event metadata error")
					continue
				}
				if filter(metadata) {
					continue
				}
				dataJson, err := crypto.Decrypt(event.Data, cryptKey(metadata.Key))
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

				log.Println("Read event: ", event.Position)
				eventChan <- Event[DT, MT]{
					Id:       event.Id,
					Type:     event.Type,
					Data:     data,
					Metadata: metadata,

					Transaction: event.Transaction,
					Position:    event.Position,
					Created:     event.Created,
				}
			}
		}
	}()
	return
}
