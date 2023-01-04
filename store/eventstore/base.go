package eventstore

import (
	"context"
	"github.com/cantara/gober/stream/event"
	"time"

	//"github.com/EventStore/EventStore-Client-Go/esdb/v2"
	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/cantara/gober/store"
)

// EventStore Adding an arbitrary 1 all revisions so revision 0 is reserved for the null condition.
type EventStore struct {
	db *esdb.Client
}

func Init() (es EventStore, err error) {
	settings, err := esdb.ParseConnectionString("esdb://localhost:2113?tls=false")
	if err != nil {
		return
	}
	db, err := esdb.NewClient(settings)
	if err != nil {
		return
	}
	es = EventStore{
		db: db,
	}
	return
}

func (es EventStore) Store(streamName string, ctx context.Context, events ...store.Event) (position uint64, err error) {
	eventDatas := make([]esdb.EventData, len(events))
	for i, e := range events {
		eventDatas[i] = esdb.EventData{
			EventID:     e.Id,
			ContentType: esdb.BinaryContentType,
			EventType:   string(e.Type),
			Data:        e.Data,
			Metadata:    e.Metadata,
		}
	}

	wr, err := es.db.AppendToStream(ctx, streamName, esdb.AppendToStreamOptions{}, eventDatas...)
	return wr.NextExpectedVersion + 1, err
}

func (es EventStore) Stream(streamName string, from store.StreamPosition, ctx context.Context) (out <-chan store.Event, err error) {
	var esFrom esdb.StreamPosition

	eventChan := make(chan store.Event, 0)
	out = eventChan
	backoff := time.Duration(1)
	go func() {
		defer close(eventChan)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			switch from {
			case store.STREAM_START:
				esFrom = esdb.Start{}
			case store.STREAM_END:
				esFrom = esdb.End{}
			default:
				esFrom = esdb.Revision(uint64(from))
			}
			stream, err := es.db.SubscribeToStream(ctx, streamName, esdb.SubscribeToStreamOptions{
				From: esFrom,
			})
			if err != nil { // Should probably atleast log this error?
				time.Sleep(backoff * time.Second)
				continue
			}
			defer stream.Close()
			subscriptionDropped := false
			for !subscriptionDropped {
				select {
				case <-ctx.Done():
					return
				default:
					subEvent := stream.Recv()
					if subEvent.SubscriptionDropped != nil {
						subscriptionDropped = true
						break
					}

					if subEvent.EventAppeared == nil {
						continue
					}

					e := subEvent.EventAppeared.OriginalEvent()
					s := store.Event{
						Id:   e.EventID,
						Type: event.TypeFromString(e.EventType),
						//Transaction: e.Position.Commit,
						Position: e.EventNumber + 1,
						Data:     e.Data,
						Metadata: e.UserMetadata,
						Created:  e.CreatedDate,
					}
					eventChan <- s
				}
			}
		}
	}()
	return
}
