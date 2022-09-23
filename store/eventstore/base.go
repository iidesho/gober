package eventstore

import (
	"context"
	"time"

	//"github.com/EventStore/EventStore-Client-Go/esdb/v2"
	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/cantara/gober/store"
)

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

func (es EventStore) Store(streamName string, ctx context.Context, events ...store.Event) (transactionId uint64, err error) {
	eventDatas := make([]esdb.EventData, len(events))
	for i, event := range events {
		eventDatas[i] = esdb.EventData{
			EventID:     event.Id,
			ContentType: esdb.BinaryContentType, //Teknically json on the back end of it
			EventType:   event.Type,
			Data:        event.Data,
			Metadata:    event.Metadata,
		}
	}

	wr, err := es.db.AppendToStream(ctx, streamName, esdb.AppendToStreamOptions{}, eventDatas...)
	return wr.CommitPosition, err //To get "true" position within local stream, the store and wait method could be moved closer to the metal, aka here rather than in the outer implementation.
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

					event := subEvent.EventAppeared.OriginalEvent()
					eventChan <- store.Event{
						Id:          event.EventID,
						Type:        event.EventType,
						Transaction: event.Position.Commit,
						Position:    event.EventNumber,
						Data:        event.Data,
						Metadata:    event.UserMetadata,
						Created:     event.CreatedDate,
					}
					if from < store.StreamPosition(event.EventNumber) { // This could potentially be a bottleneck, the only reason to have this is so that if stream end was selected, then we won't catch up the missing events in the time we were down.
						from = store.StreamPosition(event.EventNumber)
					}
				}
			}
		}
	}()
	return
}
