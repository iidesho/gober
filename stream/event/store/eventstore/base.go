package eventstore

import (
	"context"
	"errors"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/stream/event"
	"time"

	//"github.com/EventStore/EventStore-Client-Go/esdb/v2"
	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/cantara/gober/stream/event/store"
)

type Client struct {
	c *esdb.Client
}

func NewClient(host string) (c *Client, err error) {
	settings, err := esdb.ParseConnectionString(fmt.Sprintf("esdb://%s:2113?tls=false", host))
	if err != nil {
		return
	}
	esClient, err := esdb.NewClient(settings)
	if err != nil {
		return
	}
	c = &Client{
		c: esClient,
	}
	return
}

func (c *Client) Close() error {
	return c.c.Close()
}

// Stream Adding 1 to all revisions so position 0 is reserved for the null condition.
type Stream struct {
	c         *Client
	writeChan chan<- store.WriteEvent
	ctx       context.Context
	name      string
}

const BATCH_SIZE = 40000 //5000 is an arbitrary number, should probably be based on something else.

func NewStream(c *Client, stream string, ctx context.Context) (s *Stream, err error) {
	writeChan := make(chan store.WriteEvent, BATCH_SIZE)
	s = &Stream{
		c:         c,
		writeChan: writeChan,
		name:      stream,
		ctx:       ctx,
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-writeChan:
				var events []esdb.EventData
				var statusChans []chan<- event.WriteStatus
				events = append(events, esdb.EventData{
					EventID:     e.Id,
					ContentType: esdb.BinaryContentType,
					EventType:   string(e.Type),
					Data:        e.Data,
					Metadata:    e.Metadata,
				})
				if e.Status != nil {
					statusChans = append(statusChans, e.Status)
				}
				for {
					done := false
					select {
					case <-ctx.Done():
						return
					case e = <-writeChan:
						events = append(events, esdb.EventData{
							EventID:     e.Id,
							ContentType: esdb.BinaryContentType,
							EventType:   string(e.Type),
							Data:        e.Data,
							Metadata:    e.Metadata,
						})
						if e.Status != nil {
							statusChans = append(statusChans, e.Status)
						}
					default:
						done = true
					}
					if done || len(events) >= BATCH_SIZE {
						break
					}
				}

				wr, err := s.c.c.AppendToStream(ctx, s.name, esdb.AppendToStreamOptions{}, events...)
				writeStatus := event.WriteStatus{
					Error: err,
					Time:  time.Now(),
				}
				if wr != nil {
					writeStatus.Position = wr.NextExpectedVersion + 1
				}
				for _, statusChan := range statusChans {
					statusChan <- writeStatus
				}
			}
		}
	}()
	return
}

func (s *Stream) Write() chan<- store.WriteEvent {
	return s.writeChan
}

func (s *Stream) Stream(from store.StreamPosition, ctx context.Context) (out <-chan store.ReadEvent, err error) {
	var esFrom esdb.StreamPosition

	eventChan := make(chan store.ReadEvent, 10)
	out = eventChan
	backoff := time.Duration(1)
	switch from {
	case store.STREAM_START:
		esFrom = esdb.Start{}
	case store.STREAM_END:
		esFrom = esdb.End{}
	default:
		esFrom = esdb.Revision(uint64(from))
	}
	go func() {
		defer close(eventChan)
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.ctx.Done():
				return
			default:
				s.readStream(&esFrom, backoff, eventChan, ctx)
			}
		}
	}()
	return
}

func (s *Stream) readStream(esFrom *esdb.StreamPosition, backoff time.Duration, eventChan chan<- store.ReadEvent, ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Crit("recovered panic: ", r)
			s.readStream(esFrom, backoff*2, eventChan, ctx)
		}
	}()
	stream, err := s.c.c.SubscribeToStream(ctx, s.name, esdb.SubscribeToStreamOptions{
		From: *esFrom,
	})
	if err != nil {
		log.AddError(err).Debug("while subscribing to stream ", s.name)
		time.Sleep(backoff * time.Second)
		return
	}
	defer func() {
		log.AddError(stream.Close()).Debug("Closing stream")
	}() // Needed to not evaluate stream.Close() before defer is executed
	subscriptionDropped := false
	for !subscriptionDropped {
		select {
		case <-ctx.Done():
			return
		case <-s.ctx.Done():
			return
		default:
		}
		subEvent := stream.Recv()
		if subEvent.SubscriptionDropped != nil {
			subscriptionDropped = true
			break
		}

		if subEvent.EventAppeared == nil {
			continue
		}

		e := subEvent.EventAppeared.OriginalEvent()
		es := store.ReadEvent{
			Event: store.Event{
				Id:       e.EventID,
				Type:     event.TypeFromString(e.EventType),
				Data:     e.Data,
				Metadata: e.UserMetadata,
			},
			//Transaction: e.Position.Commit,
			Position: e.EventNumber + 1,
			Created:  e.CreatedDate,
		}
		*esFrom = esdb.Revision(e.EventNumber)
		eventChan <- es
	}
}

func (s *Stream) End() (pos uint64, err error) {
	//ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	//defer cancel()
	rs, err := s.c.c.ReadStream(s.ctx, s.name, esdb.ReadStreamOptions{
		Direction: esdb.Backwards,
		From:      esdb.End{},
	}, 1)
	if err != nil {
		if errors.Is(err, esdb.ErrStreamNotFound) {
			return 0, nil
		}
		return
	}
	e, err := rs.Recv()
	if err != nil {
		if errors.Is(err, esdb.ErrStreamNotFound) {
			return 0, nil
		}
		return
	}
	pos = e.Event.EventNumber
	return
}

func (s *Stream) Name() string {
	return s.name
}
