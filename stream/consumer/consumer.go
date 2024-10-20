package consumer

import (
	"context"
	"time"

	"github.com/gofrs/uuid"
	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/crypto"
	"github.com/iidesho/gober/mergedcontext"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigDefault

type consumer[BT any, T bcts.ReadWriter[BT]] struct {
	stream             stream.FilteredStream[bcts.Bytes, *bcts.Bytes]
	ctx                context.Context
	cryptoKey          stream.CryptoKeyProvider
	newTransactionChan chan transactionCheck
	completables       map[string]transactionCheck
	accChan            chan uint64
	writeStream        chan event.WriteEventReadStatus[BT, T]
	currentPosition    uint64
}

type transactionCheck struct {
	complete func()
	position uint64
}

func New[BT any, T bcts.ReadWriter[BT]](
	s stream.Stream,
	cryptoKey stream.CryptoKeyProvider,
	ctx context.Context,
) (out Consumer[BT, T], err error) {
	fs, err := stream.Init[bcts.Bytes](s, ctx)
	if err != nil {
		return
	}
	c := consumer[BT, T]{
		stream:             fs,
		cryptoKey:          cryptoKey,
		newTransactionChan: make(chan transactionCheck),
		completables:       make(map[string]transactionCheck), // NewMap[transactionCheck](),
		accChan:            make(chan uint64),                 // 1000),
		writeStream:        make(chan event.WriteEventReadStatus[BT, T]),
		ctx:                ctx,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case completable := <-c.newTransactionChan:
				if c.currentPosition >= completable.position {
					completable.complete()
					// close(completeChan.completeChan) // <- struct{}{}
					continue
				}
				c.completables[uuid.Must(uuid.NewV7()).String()] = completable
				// c.completeChans.Store(uuid.Must(uuid.NewV7()).String(), completeChan)
			case position := <-c.accChan:
				if c.currentPosition < position {
					c.currentPosition = position
				}
				for id, completable := range c.completables {
					if position < completable.position {
						continue
					}
					completable.complete()
					// close(completable.completeChan) // <- struct{}{}
					delete(c.completables, id)
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

func (c *consumer[BT, T]) Write() chan<- event.WriteEventReadStatus[BT, T] {
	return c.writeStream
}

func (c *consumer[BT, T]) Stream(
	eventTypes []event.Type,
	from store.StreamPosition,
	filter stream.Filter,
	ctx context.Context,
) (out <-chan event.ReadEventWAcc[BT, T], err error) {
	return c.streamReadEvents(eventTypes, from, filter, ctx)
}

func (c *consumer[BT, T]) store(e event.WriteEventReadStatus[BT, T]) (position uint64, err error) {
	es, err := EncryptEvent[BT, T](e.Event(), c.cryptoKey)
	if err != nil {
		return
	}

	position, err = c.stream.Store(es)
	if err != nil {
		return
	}
	c.newTransactionChan <- transactionCheck{
		position: position,
		complete: func() {
			e.Close(store.WriteStatus{
				Position: position,
				Time:     time.Now(),
			})
		},
	}
	return
}

func (c *consumer[BT, T]) streamWriteEvents(
	eventStream <-chan event.WriteEventReadStatus[BT, T],
) (err error) {
	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case e := <-eventStream:
				p, err := c.store(e)
				log.WithError(err).Debug("store", "pos", p)

			}
		}
	}()
	return
}

func (c *consumer[BT, T]) streamReadEvents(
	eventTypes []event.Type,
	from store.StreamPosition,
	filter stream.Filter,
	ctx context.Context,
) (out <-chan event.ReadEventWAcc[BT, T], err error) {
	mctx, cancel := mergedcontext.MergeContexts(c.ctx, ctx)
	s, err := c.stream.Stream(eventTypes, from, filter, mctx)
	if err != nil {
		cancel()
		return
	}
	eventChan := make(chan event.ReadEventWAcc[BT, T])
	out = eventChan
	go func() {
		defer cancel()
		for {
			select {
			case <-mctx.Done():
				return
			case e := <-s:
				o, err := DecryptEvent[BT, T](e, c.cryptoKey)
				if err != nil {
					log.WithError(err).Error("while reading event")
					continue
				}
				eventChan <- event.ReadEventWAcc[BT, T]{
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

func (c *consumer[BT, T]) End() (pos uint64, err error) {
	return c.stream.End()
}

func (c *consumer[BT, T]) Name() string {
	return c.stream.Name()
}

func (c *consumer[BT, T]) FilteredEnd(
	eventTypes []event.Type,
	filter stream.Filter,
) (pos uint64, err error) {
	return c.stream.FilteredEnd(eventTypes, filter)
}

func EncryptEvent[BT any, T bcts.ReadWriter[BT]](
	e *event.Event[BT, T],
	cryptoKey stream.CryptoKeyProvider,
) (es event.Event[bcts.Bytes, *bcts.Bytes], err error) {
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
	es = *ev.Event()
	return
}

func DecryptEvent[BT any, T bcts.ReadWriter[BT]](
	e event.ReadEvent[bcts.Bytes, *bcts.Bytes],
	cryptoKey stream.CryptoKeyProvider,
) (out event.ReadEvent[BT, T], err error) {
	dataJson, err := crypto.Decrypt(*e.Data, cryptoKey(e.Metadata.Key))
	if err != nil {
		log.WithError(err).Warning("Decrypting event data error")
		return
	}
	var data T
	err = json.Unmarshal(dataJson, &data)
	if err != nil {
		log.WithError(err).Warning("Unmarshalling event data error")
		return
	}
	out = event.ReadEvent[BT, T]{
		Event: event.Event[BT, T]{
			Type:     e.Type,
			Data:     data,
			Metadata: e.Metadata,
		},
		Position: e.Position,
		Created:  e.Created,
	}
	return
}
