package stream

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/mergedcontext"
	// jsoniter "github.com/json-iterator/go"

	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
)

var log = sbragi.WithLocalScope(sbragi.LevelError)

// var json = jsoniter.ConfigDefault

type eventService[BT any, T bcts.ReadWriter[BT]] struct {
	store  Stream
	writes chan<- event.WriteEventReadStatus[BT, T]
	ctx    context.Context
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

func Init[BT any, T bcts.ReadWriter[BT]](
	st Stream,
	ctx context.Context,
) (out FilteredStream[BT, T], err error) {
	writes := make(chan event.WriteEventReadStatus[BT, T])
	es := eventService[BT, T]{
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

func (es eventService[BT, T]) Write() chan<- event.WriteEventReadStatus[BT, T] {
	return es.writes
}

func (es eventService[BT, T]) Store(e event.Event[BT, T]) (position uint64, err error) {
	we := event.NewWriteEvent(e)
	es.writes <- we
	s := <-we.Done()
	return s.Position, s.Error
}

func (es eventService[BT, T]) Stream(
	eventTypes []event.Type,
	from store.StreamPosition,
	filter Filter,
	ctx context.Context,
) (out <-chan event.ReadEvent[BT, T], err error) {
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
	eventChan := make(chan event.ReadEvent[BT, T])
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
				log.Trace("read event", "type", t)
				if filterEventTypes {
					if _, ok := ets[t]; !ok {
						log.Debug("filtered event", "type", t)
						continue
					}
				}
				var metadata event.Metadata
				err := metadata.ReadBytes(bytes.NewReader(e.Metadata))
				// err := json.Unmarshal(e.Metadata, &metadata)
				log.WithError(err).
					Trace("Unmarshalling event metadata", "event", string(e.Metadata), "metadata", metadata)
				if err != nil {
					continue
				}
				if filter(metadata) {
					log.Debug("Filtering metadata", "metadata", metadata)
					continue
				}
				// var d T
				// err = json.Unmarshal(e.Data, &d)
				d, err := bcts.Read[BT, T](e.Data)
				log.WithError(err).
					Trace("Unmarshalling event data", "event", string(e.Data), "data", d)
				if err != nil {
					continue
				}

				eventChan <- event.ReadEvent[BT, T]{
					Event: event.Event[BT, T]{
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

func (es eventService[BT, T]) Name() string {
	return es.store.Name()
}

func (es eventService[BT, T]) End() (pos uint64, err error) {
	return es.store.End()
}

func (es eventService[BT, T]) FilteredEnd(
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
	log.WithError(err).Info("got stream end", "end", end, "stream", es.Name())
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
		err := metadata.ReadBytes(bytes.NewReader(e.Metadata))
		// err := json.Unmarshal(e.Metadata, &metadata)
		log.WithError(err).
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
