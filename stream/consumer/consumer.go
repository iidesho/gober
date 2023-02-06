package consumer

import (
	"context"
	"encoding/json"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/gofrs/uuid"
)

type consumer[T any] struct {
	stream             stream.Stream
	cryptoKey          stream.CryptoKeyProvider
	newTransactionChan chan transactionCheck
	currentPosition    uint64
	completeChans      map[string]transactionCheck
	accChan            chan uint64
	ctx                context.Context
}

type transactionCheck struct {
	position     uint64
	completeChan chan struct{}
}

func New[T any](s stream.Stream, cryptoKey stream.CryptoKeyProvider, eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out Consumer[T], eventStream <-chan ReadEvent[T], err error) {
	c := consumer[T]{
		stream:             s,
		cryptoKey:          cryptoKey,
		newTransactionChan: make(chan transactionCheck, 0),
		completeChans:      make(map[string]transactionCheck), //NewMap[transactionCheck](),
		accChan:            make(chan uint64, 0),              //1000),
		ctx:                ctx,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case completeChan := <-c.newTransactionChan:
				if c.currentPosition >= completeChan.position {
					completeChan.completeChan <- struct{}{}
					continue
				}
				c.completeChans[uuid.Must(uuid.NewV7()).String()] = completeChan
				//c.completeChans.Store(uuid.Must(uuid.NewV7()).String(), completeChan)
			case position := <-c.accChan:
				if c.currentPosition < position {
					c.currentPosition = position
				}
				for id, completeChan := range c.completeChans {
					if position < completeChan.position {
						continue
					}
					completeChan.completeChan <- struct{}{}
					delete(c.completeChans, id)
				}
				/*
					c.completeChans.Range(func(id string, completeChan transactionCheck) bool {
						if position < completeChan.position {
							return true
						}
						completeChan.completeChan <- struct{}{}
						c.completeChans.Delete(id)
						return true
					})
				*/
			}
		}
	}()

	eventStream, err = c.streamReadEvents(eventTypes, from, filter)
	if err != nil {
		return
	}

	out = &c
	return
}

func (c *consumer[T]) Store(e Event[T]) (position uint64, err error) {
	es, err := EncryptEvent[T](e, c.cryptoKey)
	if err != nil {
		return
	}
	position, err = c.stream.Store(es)
	if err != nil {
		return
	}

	completeChan := make(chan struct{})
	defer close(completeChan)
	c.newTransactionChan <- transactionCheck{
		position:     position,
		completeChan: completeChan,
	}
	<-completeChan
	return
}

func (c *consumer[T]) streamReadEvents(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter) (out <-chan ReadEvent[T], err error) {
	s, err := c.stream.Stream(eventTypes, from, filter, c.ctx)
	if err != nil {
		return
	}
	eventChan := make(chan ReadEvent[T], 0)
	out = eventChan
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case e := <-s:
				o, err := DecryptEvent[T](e, c.cryptoKey)
				if err != nil {
					log.AddError(err).Error("while reading event")
					continue
				}
				o.Acc = func() {
					c.accChan <- o.Position
				}
				o.CTX = c.ctx
				eventChan <- o
			}
		}
	}()
	return
}

func (c *consumer[T]) End() (pos uint64, err error) {
	return c.stream.End()
}

func (c *consumer[T]) Name() string {
	return c.stream.Name()
}

func EncryptEvent[T any](e Event[T], cryptoKey stream.CryptoKeyProvider) (es event.Event, err error) {
	data, err := json.Marshal(e.Data)
	if err != nil {
		return
	}
	edata, err := crypto.Encrypt(data, cryptoKey(e.Metadata.Key))
	if err != nil {
		return
	}
	es, err = event.NewBuilder().
		WithType(e.Type).
		WithMetadata(e.Metadata).
		WithData(edata).
		BuildStore()
	if err != nil {
		return
	}
	return
}

func DecryptEvent[T any](e event.ReadEvent, cryptoKey stream.CryptoKeyProvider) (out ReadEvent[T], err error) {
	dataJson, err := crypto.Decrypt(e.Data, cryptoKey(e.Metadata.Key))
	if err != nil {
		log.AddError(err).Warning("Decrypting event data error")
		return
	}
	var data T
	err = json.Unmarshal(dataJson, &data)
	if err != nil {
		log.AddError(err).Warning("Unmarshalling event data error")
		return
	}
	out = ReadEvent[T]{
		Event: Event[T]{
			Type:     e.Type,
			Data:     data,
			Metadata: e.Metadata,
		},
		Position: e.Position,
		Created:  e.Created,
	}
	return
}
