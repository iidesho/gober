package competing

import (
	"context"
	"errors"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/event"
	"github.com/gofrs/uuid"
	"time"
)

type competing[T any] struct {
	stream          stream.Stream
	cryptoKey       stream.CryptoKeyProvider
	accChan         chan uuid.UUID
	timeoutChan     chan timeout
	timedOutChan    chan uuid.UUID
	competers       consumer.Map[consumer.Event[handleType[T]]]
	timeout         time.Duration
	name            uuid.UUID
	catchupPosition uint64
	ctx             context.Context
}

type timeout struct {
	ID   uuid.UUID
	When time.Time
}

type handleType[T any] struct {
	ID   uuid.UUID `json:"id"`
	Data T         `json:"data"`
	Name uuid.UUID `json:"name"`
}

func New[T any](s stream.Stream, cryptoKey stream.CryptoKeyProvider, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out consumer.Consumer[T], eventStream <-chan consumer.ReadEvent[T], err error) {
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	c := competing[T]{
		stream:       s,
		cryptoKey:    cryptoKey,
		accChan:      make(chan uuid.UUID, 0), //1000),
		timedOutChan: make(chan uuid.UUID, 1000),
		timeoutChan:  make(chan timeout, 1000),
		competers:    consumer.NewMap[consumer.Event[handleType[T]]](),
		timeout:      time.Minute,
		name:         name,
		ctx:          ctx,
	}

	eventStream, err = c.readStream(event.AllTypes(), from, filter)
	if err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case id := <-c.accChan:
				_, err = c.store(consumer.Event[handleType[T]]{
					Type: event.Delete,
					Data: handleType[T]{
						ID: id,
					},
				})
				if err != nil {
					log.AddError(err).Error("while storing finished competing consumer")
					continue
				}
			case newTimeout := <-c.timeoutChan:
				go func() {
					select {
					case <-c.ctx.Done():
						return
					case <-time.After(newTimeout.When.Sub(time.Now())):
						c.timedOutChan <- newTimeout.ID
					}
				}()
			case id := <-c.timedOutChan:
				e, ok := c.competers.Load(id.String())
				if !ok {
					continue
				}
				c.compete(e)
			}
		}
	}()

	out = &c
	return
}

func (c *competing[T]) Store(e consumer.Event[T]) (position uint64, err error) {
	if e.Type != "" && e.Type != event.Create {
		err = errors.New("event type should always be create or empty when using competing consumers")
		return
	}
	id, err := uuid.NewV7()
	if err != nil {
		return
	}
	es := consumer.Event[handleType[T]]{
		Type: event.Create,
		Data: handleType[T]{
			ID:   id,
			Data: e.Data,
		},
		Metadata: e.Metadata,
	}
	return c.store(es)
}

func (c *competing[T]) store(e consumer.Event[handleType[T]]) (position uint64, err error) {
	es, err := consumer.EncryptEvent[handleType[T]](e, c.cryptoKey)
	if err != nil {
		return
	}
	return c.stream.Store(es)
}

func (c *competing[T]) readStream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter) (out <-chan consumer.ReadEvent[T], err error) {
	s, err := c.stream.Stream(eventTypes, from, filter, c.ctx)
	if err != nil {
		return
	}
	eventChan := make(chan consumer.ReadEvent[T], 0)
	out = eventChan
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case e := <-s:
				o, err := consumer.DecryptEvent[handleType[T]](e, c.cryptoKey)
				if err != nil {
					log.AddError(err).Error("while reading event")
					continue
				}
				if o.Type == event.Delete {
					c.competers.Delete(o.Data.ID.String())
					continue
				}

				c.competers.Store(o.Data.ID.String(), o.Event)
				c.timeoutChan <- timeout{
					ID:   o.Data.ID,
					When: o.Created.Add(c.timeout),
				}

				if o.Type == event.Create {
					if o.Position > c.catchupPosition {
						c.compete(o.Event)
					}
					continue
				}
				if o.Data.Name != c.name {
					continue
				}
				ctx, cancel := context.WithTimeout(c.ctx, c.timeout-time.Now().Sub(o.Created)-10)
				eventChan <- consumer.ReadEvent[T]{
					Event: consumer.Event[T]{
						Type:     o.Type,
						Data:     o.Data.Data,
						Metadata: o.Metadata,
					},
					Position: o.Position,
					Created:  o.Created,
					Acc: func() {
						c.accChan <- o.Data.ID
						cancel()
					},
					CTX: ctx,
				}
			}
		}
	}()
	return
}

func (c *competing[T]) End() (pos uint64, err error) {
	return c.stream.End()
}

func (c *competing[T]) Name() string {
	return c.stream.Name()
}

func (c *competing[T]) compete(e consumer.Event[handleType[T]]) {
	e.Type = event.Update
	e.Data.Name = c.name
	es, err := consumer.EncryptEvent[handleType[T]](e, c.cryptoKey)
	if err != nil {
		log.AddError(err).Error("while encrypting during compete")
		return
	}
	_, err = c.stream.Store(es)
	if err != nil {
		log.AddError(err).Warning("while trying to compete")
		return
	}
}
