package stream

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/mergedcontext"
	jsoniter "github.com/json-iterator/go"

	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
)

var json = jsoniter.ConfigDefault

type eventService[T any] struct {
	store  Stream
	writes chan<- event.WriteEventReadStatus[T]
	ctx    context.Context
}

type Filter func(md event.Metadata) bool

type CryptoKeyProvider func(key string) sbragi.RedactedString

func StaticProvider(key sbragi.RedactedString) func(_ string) sbragi.RedactedString {
	return func(_ string) sbragi.RedactedString {
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

func Init[T any](st Stream, ctx context.Context) (out FilteredStream[T], err error) {
	writes := make(chan event.WriteEventReadStatus[T])
	es := eventService[T]{
		store:  st,
		writes: writes,
		ctx:    ctx,
	}
	out = es
	go func() {
		for we := range writes {
			e := we.Event()
			if e.Type == event.Invalid {
				we.Close(store.WriteStatus{
					Error: fmt.Errorf("event type %s, error:%v", e.Type, event.ErrInvalidType),
				})
				continue
			}
			e.Metadata.Stream = es.store.Name()
			e.Metadata.EventType = e.Type
			e.Metadata.Created = time.Now()
			se := we.Store()
			if se == nil {
				continue
			}
			es.store.Write() <- *se
		}
	}()
	return
}

func (es eventService[T]) Write() chan<- event.WriteEventReadStatus[T] {
	return es.writes
}

func (es eventService[T]) Store(e event.Event[T]) (position uint64, err error) {
	we := event.NewWriteEvent(e)
	es.writes <- we
	s := <-we.Done()
	return s.Position, s.Error
}

func (es eventService[T]) Stream(
	eventTypes []event.Type,
	from store.StreamPosition,
	filter Filter,
	ctx context.Context,
) (out <-chan event.ReadEvent[T], err error) {
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
	eventChan := make(chan event.ReadEvent[T])
	out = eventChan
	go func() {
		defer cancel()
		defer close(eventChan)
		for {
			select {
			case <-mctx.Done():
				return
			case e := <-s:
				t := event.TypeFromString(e.Type)
				sbragi.Trace("read event", "type", t)
				if filterEventTypes {
					if _, ok := ets[t]; !ok {
						sbragi.Debug("filtered event", "type", t)
						continue
					}
				}
				var metadata event.Metadata
				err := metadata.ReadBytes(bytes.NewReader(e.Metadata))
				//err := json.Unmarshal(e.Metadata, &metadata)
				sbragi.WithError(err).
					Trace("Unmarshalling event metadata", "event", string(e.Metadata), "metadata", metadata)
				if err != nil {
					continue
				}
				if filter(metadata) {
					sbragi.Debug("Filtering metadata", "metadata", metadata)
					continue
				}
				var d T
				err = json.Unmarshal(e.Data, &d)
				sbragi.WithError(err).
					Trace("Unmarshalling event data", "event", string(e.Data), "data", d)
				if err != nil {
					continue
				}

				eventChan <- event.ReadEvent[T]{
					Event: event.Event[T]{
						Type:     t,
						Data:     d,
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

func (es eventService[T]) Name() string {
	return es.store.Name()
}

func (es eventService[T]) End() (pos uint64, err error) {
	return es.store.End()
}

func (es eventService[T]) FilteredEnd(
	eventTypes []event.Type,
	filter Filter,
) (pos uint64, err error) {
	filterEventTypes := len(eventTypes) > 0
	ets := make(map[event.Type]struct{})
	for _, eventType := range eventTypes {
		ets[eventType] = struct{}{}
	}
	p := uint64(0)
	end, err := es.End()
	if err != nil {
		return
	}
	s, err := es.store.Stream(store.STREAM_START, es.ctx)
	if err != nil {
		return
	}
	sbragi.WithError(err).Info("got stream end", "end", end, "stream", es.Name())
	for p < end {
		e := <-s
		p = e.Position
		t := event.TypeFromString(e.Type)
		if filterEventTypes {
			if _, ok := ets[t]; !ok {
				continue
			}
		}
		var metadata event.Metadata
		err := json.Unmarshal(e.Metadata, &metadata)
		sbragi.WithError(err).
			Debug("Unmarshalling event metadata", "event", string(e.Metadata), "metadata", metadata)
		if err != nil {
			continue
		}
		if filter(metadata) {
			continue
		}
		pos = p
	}
	return
}
