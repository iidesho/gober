package stream

import (
	"context"
	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/mergedcontext"
	jsoniter "github.com/json-iterator/go"
	"time"

	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
)

var json = jsoniter.ConfigDefault

type eventService struct {
	store Stream
	ctx   context.Context
}

type Filter func(md event.Metadata) bool

type CryptoKeyProvider func(key string) log.RedactedString

func StaticProvider(key log.RedactedString) func(_ string) log.RedactedString {
	return func(_ string) log.RedactedString {
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

func Init(st Stream, ctx context.Context) (out FilteredStream, err error) {
	es := eventService{
		store: st,
		ctx:   ctx,
	}
	out = es
	return
}

func (es eventService) Write() chan<- event.ByteWriteEvent {
	writeEvent := make(chan event.ByteWriteEvent, 0)
	go func() {
		for e := range writeEvent {
			if event.TypeFromString(string(e.Type)) == event.Invalid {
				if e.Status != nil {
					e.Status <- event.WriteStatus{
						Error: event.InvalidTypeError,
					}
				}
				continue
			}
			e.Metadata.Stream = es.store.Name()
			e.Metadata.EventType = e.Type
			e.Metadata.Created = time.Now()
			metadataByte, err := json.Marshal(e.Metadata)
			if err != nil {
				if e.Status != nil {
					e.Status <- event.WriteStatus{
						Error: err,
					}
				}
				continue
			}
			es.store.Write() <- store.WriteEvent{
				Event: store.Event{
					Type:     e.Type,
					Data:     e.Data,
					Metadata: metadataByte,
				},
				Status: e.Status,
			}
		}
	}()
	return writeEvent
}

func (es eventService) Store(e event.ByteEvent) (transactionId uint64, err error) {
	if event.TypeFromString(string(e.Type)) == event.Invalid {
		err = event.InvalidTypeError
		return
	}
	e.Metadata.Stream = es.store.Name()
	e.Metadata.EventType = e.Type
	e.Metadata.Created = time.Now()
	metadataByte, err := json.Marshal(e.Metadata)
	if err != nil {
		return
	}
	status := make(chan event.WriteStatus, 1)
	es.store.Write() <- store.WriteEvent{
		Event: store.Event{
			Type:     e.Type,
			Data:     e.Data,
			Metadata: metadataByte,
		},
		Status: status,
	}
	s := <-status
	return s.Position, s.Error
}

func (es eventService) Stream(eventTypes []event.Type, from store.StreamPosition, filter Filter, ctx context.Context) (out <-chan event.ByteReadEvent, err error) {
	filterEventTypes := len(eventTypes) > 0
	ets := make(map[event.Type]struct{})
	for _, eventType := range eventTypes {
		ets[eventType] = struct{}{}
	}
	mctx, cancel := mergedcontext.MergeContexts(es.ctx, ctx)
	s, err := es.store.Stream(from, mctx)
	if err != nil {
		cancel()
		return
	}
	eventChan := make(chan event.ByteReadEvent, 0)
	out = eventChan
	go func() {
		defer cancel()
		defer close(eventChan)
		for {
			select {
			case <-mctx.Done():
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
					log.WithError(err).Warning("Unmarshalling event metadata error")
					continue
				}
				if filter(metadata) {
					continue
				}

				eventChan <- event.ByteReadEvent{
					Event: event.Event[[]byte]{
						Type:     e.Type,
						Data:     e.Data,
						Metadata: metadata,
					},

					Position: e.Position,
					Created:  e.Created,
				}
			}
		}
	}()
	return
}

func (es eventService) Name() string {
	return es.store.Name()
}

func (es eventService) End() (pos uint64, err error) {
	return es.store.End()
}
