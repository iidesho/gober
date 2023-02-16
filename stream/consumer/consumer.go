package consumer

import (
	"context"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/mergedcontext"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
	"github.com/gofrs/uuid"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigDefault

type consumer[T any] struct {
	stream             stream.FilteredStream
	cryptoKey          stream.CryptoKeyProvider
	newTransactionChan chan transactionCheck
	currentPosition    uint64
	completeChans      map[string]transactionCheck
	accChan            chan uint64
	writeStream        chan event.WriteEventReadStatus[T]
	ctx                context.Context
}

type transactionCheck struct {
	position     uint64
	completeChan chan struct{}
}

func New[T any](s stream.Stream, cryptoKey stream.CryptoKeyProvider, ctx context.Context) (out Consumer[T], err error) {
	fs, err := stream.Init(s, ctx)
	if err != nil {
		return
	}
	c := consumer[T]{
		stream:             fs,
		cryptoKey:          cryptoKey,
		newTransactionChan: make(chan transactionCheck, 0),
		completeChans:      make(map[string]transactionCheck), //NewMap[transactionCheck](),
		accChan:            make(chan uint64, 0),              //1000),
		writeStream:        make(chan event.WriteEventReadStatus[T], 0),
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
			}
		}
	}()

	err = c.streamWriteEvents(c.writeStream)
	if err != nil {
		return
	}

	out = &c
	return
}

func (c *consumer[T]) Write() chan<- event.WriteEventReadStatus[T] {
	return c.writeStream
}

func (c *consumer[T]) Stream(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out <-chan event.ReadEventWAcc[T], err error) {
	return c.streamReadEvents(eventTypes, from, filter, ctx)
}

func (c *consumer[T]) store(e event.WriteEventReadStatus[T]) (position uint64, err error) {
	es, err := EncryptEvent[T](e.Event(), c.cryptoKey)
	if err != nil {
		return
	}
	position, err = c.stream.Store(es)
	if err != nil {
		return
	}

	go func() {
		completeChan := make(chan struct{})
		defer close(completeChan)
		c.newTransactionChan <- transactionCheck{
			position:     position,
			completeChan: completeChan,
		}
		<-completeChan
		e.Close()
	}()
	return
}

func (c *consumer[T]) streamWriteEvents(eventStream <-chan event.WriteEventReadStatus[T]) (err error) {
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case e := <-eventStream:
				p, err := c.store(e)
				log.AddError(err).Debug("store at pos ", p)
			}
		}
	}()
	return
}

func (c *consumer[T]) streamReadEvents(eventTypes []event.Type, from store.StreamPosition, filter stream.Filter, ctx context.Context) (out <-chan event.ReadEventWAcc[T], err error) {
	mctx, cancel := mergedcontext.MergeContexts(c.ctx, ctx)
	s, err := c.stream.Stream(eventTypes, from, filter, mctx)
	if err != nil {
		cancel()
		return
	}
	eventChan := make(chan event.ReadEventWAcc[T], 0)
	out = eventChan
	go func() {
		defer cancel()
		for {
			select {
			case <-mctx.Done():
				return
			case e := <-s:
				o, err := DecryptEvent[T](e, c.cryptoKey)
				if err != nil {
					log.AddError(err).Error("while reading event")
					continue
				}
				eventChan <- event.ReadEventWAcc[T]{
					ReadEvent: o,
					Acc: func() {
						c.accChan <- o.Position
					},
					CTX: c.ctx,
				}
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

func EncryptEvent[T any](e event.Event[T], cryptoKey stream.CryptoKeyProvider) (es event.ByteEvent, err error) {
	data, err := json.Marshal(e.Data)
	if err != nil {
		return
	}
	edata, err := crypto.Encrypt(data, cryptoKey(e.Metadata.Key))
	if err != nil {
		return
	}
	ev, err := event.NewBuilder().
		WithType(e.Type).
		WithMetadata(e.Metadata).
		WithData(edata).
		BuildStore()
	if err != nil {
		return
	}
	es = event.ByteEvent(ev.Event)
	return
}

func DecryptEvent[T any](e event.ByteReadEvent, cryptoKey stream.CryptoKeyProvider) (out event.ReadEvent[T], err error) {
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
	out = event.ReadEvent[T]{
		Event: event.Event[T]{
			Type:     e.Type,
			Data:     data,
			Metadata: e.Metadata,
		},
		Position: e.Position,
		Created:  e.Created,
	}
	return
}
