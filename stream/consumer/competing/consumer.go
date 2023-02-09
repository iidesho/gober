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
	stream         stream.Stream
	cryptoKey      stream.CryptoKeyProvider
	accChan        chan acc
	timeoutChan    chan timeout
	timedOutChan   chan consumer.Event[handleType[T]]
	competableChan chan uuid.UUID
	competable     map[string]consumer.Event[handleType[T]]
	competers      consumer.Map[consumer.Event[handleType[T]]]
	timeout        time.Duration
	name           uuid.UUID
	//catchupPosition uint64
	ctx context.Context
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

type acc struct {
	id       uuid.UUID
	version  string
	datatype string
}

func New[T any](s stream.Stream, cryptoKey stream.CryptoKeyProvider, from store.StreamPosition, datatype string, timeoutDuration time.Duration, ctx context.Context) (out consumer.Consumer[T], outChan <-chan consumer.ReadEvent[T], err error) {
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	c := competing[T]{
		stream:         s,
		cryptoKey:      cryptoKey,
		accChan:        make(chan acc, 0), //1000),
		timedOutChan:   make(chan consumer.Event[handleType[T]], 10),
		competableChan: make(chan uuid.UUID, 1000),
		timeoutChan:    make(chan timeout, 1000),
		competers:      consumer.NewMap[consumer.Event[handleType[T]]](),
		competable:     make(map[string]consumer.Event[handleType[T]]),
		timeout:        timeoutDuration,
		name:           name,
		ctx:            ctx,
	}

	eventChan := make(chan consumer.ReadEvent[T], 0)
	eventStream, err := c.readStream(event.AllTypes(), from, stream.ReadDataType(datatype))
	if err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case a := <-c.accChan:
				_, err = c.store(consumer.Event[handleType[T]]{
					Type: event.Delete,
					Data: handleType[T]{
						ID: a.id,
					},
					Metadata: event.Metadata{
						DataType: a.datatype,
						Version:  a.version,
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
						e, ok := c.competers.Load(newTimeout.ID.String())
						if !ok || e.Type != event.Update {
							return
						}
						log.Debug("TIMED OUT!! ", newTimeout.ID)
						ne, ok := c.competers.Load(newTimeout.ID.String())
						if !ok || ne.Type != event.Update {
							return
						}
						if !ne.Metadata.Created.Equal(e.Metadata.Created) {
							return
						}
						//c.timedOutChan <- e
						c.competableChan <- newTimeout.ID
					}
				}()
				//case e := <-c.timedOutChan:
				//c.competableChan <- e.Data.ID
				//c.compete(e)
			}
		}
	}()

	go func() {
		competing := false
		for {
			select {
			case <-c.ctx.Done():
				return
			case competable := <-c.competableChan:
				competer, ok := c.competers.Load(competable.String())
				if ok {
					//This filter seems to be a bit buggy when filtering out events that has timed out. Added a millisecond to the timeout check that re ads the competable
					if time.Now().After(competer.Metadata.Created.Add(c.timeout)) || competer.Metadata.EventType == event.Create {
						if competing {
							c.competableChan <- competable
							time.Sleep(time.Millisecond)
							continue
						}
						log.Debug("competing ", competable)
						c.compete(competer)
						competing = true
					}
				}
			case current := <-eventStream:
				currentctx, cancel := context.WithTimeout(c.ctx, c.timeout-time.Now().Sub(current.Metadata.Created)-(time.Second*5))
				select {
				case <-c.ctx.Done():
					return
				case <-currentctx.Done():
					//log.Debug("write timeout ", current.Data.ID)
					competing = false
				case eventChan <- consumer.ReadEvent[T]{
					Event: consumer.Event[T]{
						Type:     current.Type,
						Data:     current.Data.Data,
						Metadata: current.Metadata,
					},
					Position: current.Position,
					Acc: func() {
						defer cancel()
						if time.Now().After(current.Metadata.Created.Add(c.timeout)) {
							log.Warning("timed out before acc, discarding acc for ", current.Data.ID)
							//Should probably store this somewhere to get statistics on it.
							return
						}

						c.accChan <- acc{
							id:       current.Data.ID,
							datatype: current.Metadata.DataType,
							version:  current.Metadata.Version,
						}
					},
					CTX: currentctx,
				}:
					//log.Debug("wrote ", current.Data.ID)
					competing = false
				}
			}
		}
	}()

	outChan = eventChan
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

func (c *competing[T]) readStream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter) (out <-chan consumer.ReadEvent[handleType[T]], err error) {
	s, err := c.stream.Stream(eventTypes, from, filter, c.ctx)
	if err != nil {
		return
	}
	eventChan := make(chan consumer.ReadEvent[handleType[T]], 10)
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
					c.competers.Store(o.Data.ID.String(), o.Event)
					//c.competers.Delete(o.Data.ID.String())
					continue
				}

				lo, ok := c.competers.Load(o.Data.ID.String())
				if ok && lo.Type == event.Update && lo.Data.Name != c.name && lo.Metadata.Created.Add(c.timeout).After(time.Now()) {
					continue
				}
				c.competers.Store(o.Data.ID.String(), o.Event)

				if o.Type == event.Create {
					//c.compete(o.Event)
					c.competableChan <- o.Event.Data.ID
					continue
				}

				c.timeoutChan <- timeout{
					ID:   o.Data.ID,
					When: o.Metadata.Created.Add(c.timeout + time.Millisecond),
				}
				if o.Data.Name != c.name {
					continue
				}
				eventChan <- o
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
