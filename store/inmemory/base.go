package inmemory

import (
	"context"
	log "github.com/cantara/bragi"
	"sync"
	"time"

	"github.com/cantara/gober/store"
)

type Stream struct {
	db       []store.Event
	dbLock   *sync.Mutex
	newData  *sync.Cond
	position *uint64
}

type EventStore struct {
	streams sync.Map
}

func Init() (es *EventStore, err error) {
	es = &EventStore{
		streams: sync.Map{},
	}
	return
}

func (es *EventStore) Store(streamName string, ctx context.Context, events ...store.Event) (transactionId uint64, err error) {
	streamAny, _ := es.streams.LoadOrStore(streamName, Stream{
		db:      make([]store.Event, 0),
		dbLock:  &sync.Mutex{},
		newData: sync.NewCond(&sync.Mutex{}),
	})
	stream := streamAny.(Stream)
	stream.dbLock.Lock()
	defer stream.dbLock.Unlock()
	streamAny, _ = es.streams.Load(streamName)

	trans := uint64(time.Now().UnixNano())

	curPos := len(stream.db)

	for i := range events {
		events[i].Transaction = trans
		events[i].Position = uint64(curPos + i)
	}

	stream.db = append(stream.db, events...)
	p := uint64(curPos + len(events))
	stream.position = &p

	//stream.dbLock.Unlock()
	es.streams.Store(streamName, stream)
	stream.newData.Broadcast()
	return trans, nil
}

func (es *EventStore) Stream(streamName string, from store.StreamPosition, ctx context.Context) (out <-chan store.Event, err error) {
	streamAny, _ := es.streams.LoadOrStore(streamName, Stream{
		db:      make([]store.Event, 0),
		dbLock:  &sync.Mutex{},
		newData: sync.NewCond(&sync.Mutex{}),
	})
	stream := streamAny.(Stream)
	log.Debug(stream)

	eventChan := make(chan store.Event, 2)
	out = eventChan
	go func() {
		defer close(eventChan)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			position := uint64(from)
			if from == store.STREAM_END {
				position = uint64(len(stream.db))
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
					for ; position < uint64(len(stream.db)); position++ {
						eventChan <- stream.db[position]
					}
					streamAny, _ = es.streams.Load(streamName)
					stream = streamAny.(Stream)
					if position >= uint64(len(stream.db)) {
						stream.newData.L.Lock()
						stream.newData.Wait()
						stream.newData.L.Unlock()
					}
				}
			}
		}
	}()
	return
}
