package stream

import (
	"context"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/stream/event"
	"github.com/gofrs/uuid"
	"time"
)

type SetHelper interface {
	SetAndWait(event.StoreEvent) error
}

type transaction[DT any] struct {
	stream             Stream
	keyProvider        CryptoKeyProvider
	eventChannel       <-chan event.Event[DT]
	newTransactionChan chan transactionCheck
	currentTransaction uint64
	completeChans      Map[transactionCheck] //Could potentially be changed to native map again
	store              func(event.Event[DT])
	delete             func(event.Event[DT])
	ctx                context.Context
}

func InitSetHelper[DT any](store, delete func(event.Event[DT]), stream Stream, keyProvider CryptoKeyProvider, eventChannel <-chan event.Event[DT], ctx context.Context) (out SetHelper) {
	t := transaction[DT]{
		stream:             stream,
		keyProvider:        keyProvider,
		eventChannel:       eventChannel,
		newTransactionChan: make(chan transactionCheck),
		completeChans:      New[transactionCheck](),
		store:              store,
		delete:             delete,
		ctx:                ctx,
	}

	finishedTransactionChan := make(chan uint64, 5)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case completeChan := <-t.newTransactionChan:
				t.completeTransaction(completeChan)
			case position := <-finishedTransactionChan:
				t.finishedPosition(position)
			}
		}
	}()
	t.readStream(finishedTransactionChan)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-t.eventChannel:
				t.handleTransaction(e, finishedTransactionChan)
			}
		}
	}()

	return &t
}

type transactionCheck struct {
	position     uint64
	completeChan chan struct{}
}

func (t *transaction[DT]) readStream(finishedTransactionChan chan<- uint64) {
	upToDate := false
	readOut := false
	for !upToDate {
		select {
		case <-t.ctx.Done():
			return
		case e := <-t.eventChannel:
			t.handleTransaction(e, finishedTransactionChan)
			readOut = false
		default:
			time.Sleep(10 * time.Millisecond)
			if !readOut {
				readOut = true
				continue
			}
			upToDate = true
		}
	}
}

func (t *transaction[DT]) handleTransaction(e event.Event[DT], finishedTransactionChan chan<- uint64) {
	defer func() {
		log.Debug("written position ", e.Position)
		finishedTransactionChan <- e.Position
	}()
	if e.Type == event.Delete {
		log.Debug("starting delete for ", e.Position)
		t.delete(e)
		log.Debug("finished delete for ", e.Position)
		return
	}
	log.Debug("starting store for ", e.Position)
	t.store(e)
	log.Debug("finished store for ", e.Position)
}

func (t *transaction[DT]) completeTransaction(completeChan transactionCheck) {
	log.Debug("cur ", t.currentTransaction, " pos ", completeChan.position)
	if t.currentTransaction >= completeChan.position {
		log.Debug("since we have already read this position before it wanted to be verified we say that it is completed", t.currentTransaction, completeChan.position)
		completeChan.completeChan <- struct{}{}
		return
	}
	log.Debug("storing the new complete chan")
	t.completeChans.Store(uuid.Must(uuid.NewV7()).String(), completeChan)
}

func (t *transaction[DT]) finishedPosition(position uint64) {
	if t.currentTransaction < position {
		t.currentTransaction = position
		log.Debug("new cur ", t.currentTransaction, " pos ", position)
	} else {
		log.Warning("Seems that the new position was not newer than the previous one. ", fmt.Sprintf("%d < %d", t.currentTransaction, position))
	}
	t.completeChans.Range(func(id string, completeChan transactionCheck) bool {
		log.Debug(position, completeChan.position)
		if position < completeChan.position {
			return true
		}
		completeChan.completeChan <- struct{}{}
		t.completeChans.Delete(id)
		return true
	})
}

func (t *transaction[DT]) verifyAllWrites(finishedTransactionChan <-chan uint64) {
	upToDate := false
	for !upToDate {
		select {
		case <-t.ctx.Done():
			return
		case completeChan := <-t.newTransactionChan:
			t.completeTransaction(completeChan)
		case position := <-finishedTransactionChan:
			t.finishedPosition(position)
		default:
			upToDate = true
		}
	}
}

func (t *transaction[DT]) SetAndWait(e event.StoreEvent) (err error) {
	position, err := t.stream.Store(e, t.keyProvider)
	if err != nil {
		return
	}
	completeChan := make(chan struct{})
	defer close(completeChan)
	t.newTransactionChan <- transactionCheck{
		position:     position,
		completeChan: completeChan,
	}
	log.Debug("Set and wait waiting for ", position)
	<-completeChan
	return
}
