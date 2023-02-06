package stream

import (
	"context"
	"encoding/json"
	log "github.com/cantara/bragi"
	"time"

	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream/event"
)

type eventService struct {
	store      Persistence
	streamName string
	writeChan  chan eventWrite
}

type eventWrite struct {
	event      store.Event
	returnChan chan eventWriteReturn
}

type eventWriteReturn struct {
	position uint64
	err      error
}

type Filter func(md event.Metadata) bool

type CryptoKeyProvider func(key string) string

func StaticProvider(key string) func(_ string) string {
	return func(_ string) string {
		return key
	}
}

func ReadAll() Filter {
	return func(_ event.Metadata) bool { return false }
}

func ReadEventType(t event.Type) Filter {
	return func(md event.Metadata) bool { return md.EventType != t }
}

func ReadDataType(t string) Filter {
	return func(md event.Metadata) bool { return md.DataType != t }
}

const BATCH_SIZE = 5000 //5000 is an arbitrary number, should probably be based on something else.

func Init(st Persistence, stream string, ctx context.Context) (out Stream, err error) {
	es := eventService{
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

				position, err := es.store.Store(es.streamName, ctx, events...)
				for _, returnChan := range returnChans {
					returnChan <- eventWriteReturn{
						position: position,
						err:      err,
					}
				}
				events = nil
				returnChans = nil
			}
		}
	}()
	return
}

func (es eventService) Store(e event.Event) (transactionId uint64, err error) {
	if event.TypeFromString(string(e.Type)) == event.Invalid {
		err = event.InvalidTypeError
		return
	}
	e.Metadata.Stream = es.streamName
	e.Metadata.EventType = e.Type
	e.Metadata.Created = time.Now()
	metadataByte, err := json.Marshal(e.Metadata)
	if err != nil {
		return
	}

	eventData := store.Event{
		Id:       e.Id,
		Type:     e.Type,
		Data:     e.Data,
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

	return writeReturn.position, writeReturn.err
}

func (es eventService) Stream(eventTypes []event.Type, from store.StreamPosition, filter Filter, ctx context.Context) (out <-chan event.ReadEvent, err error) {
	filterEventTypes := len(eventTypes) > 0
	ets := make(map[event.Type]struct{})
	for _, eventType := range eventTypes {
		ets[eventType] = struct{}{}
	}
	s, err := es.store.Stream(es.streamName, from, ctx)
	if err != nil {
		return
	}
	eventChan := make(chan event.ReadEvent, BATCH_SIZE)
	out = eventChan
	go func() {
		defer close(eventChan)
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-s:
				if filterEventTypes {
					if _, ok := ets[e.Type]; !ok {
						continue
					}
				}
				var metadata event.Metadata
				err := json.Unmarshal(e.Metadata, &metadata)
				if err != nil {
					log.AddError(err).Warning("Unmarshalling event metadata error")
					continue
				}
				if filter(metadata) {
					continue
				}

				eventChan <- event.ReadEvent{
					Event: event.Event{
						Id:       e.Id,
						Type:     e.Type,
						Data:     e.Data,
						Metadata: metadata,
					},

					//Transaction: e.Transaction,
					Position: e.Position,
					Created:  e.Created,
				}
			}
		}
	}()
	return
}

func (es eventService) Name() string {
	return es.streamName
}

func (es eventService) End() (pos uint64, err error) {
	return es.store.EndPosition(es.streamName)
}
