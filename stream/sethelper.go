package stream

import (
	"context"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/stream/event"
	"github.com/gofrs/uuid"
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
			default:
				t.verifyWrite(finishedTransactionChan)
			}
		}
	}()
	t.readStream(finishedTransactionChan)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				t.readStream(finishedTransactionChan)
			}
		}
	}()

	return &t
}

type transactionCheck struct {
	transaction  uint64
	completeChan chan struct{}
}

func (t *transaction[DT]) readStream(finishedTransactionChan chan<- uint64) {
	upToDate := false
	for !upToDate {
		select {
		case <-t.ctx.Done():
			return
		case e := <-t.eventChannel:
			t.handleTransaction(e, finishedTransactionChan)
		default:
			upToDate = true
		}
	}
}

func (t *transaction[DT]) handleTransaction(e event.Event[DT], finishedTransactionChan chan<- uint64) {
	defer func() {
		log.Debug("written transaction ", e.Transaction)
		finishedTransactionChan <- e.Transaction
	}()
	if e.Type == event.Delete {
		t.delete(e)
		return
	}
	t.store(e)
}

func (t *transaction[DT]) verifyWrite(finishedTransactionChan <-chan uint64) {
	upToDate := false
	for !upToDate {
		select {
		case <-t.ctx.Done():
			return
		case completeChan := <-t.newTransactionChan:
			if t.currentTransaction >= completeChan.transaction {
				log.Debug("since we have already read this transaction before it wanted to be verified we say that it is completed")
				completeChan.completeChan <- struct{}{}
				return
			}
			log.Debug("storing the new comple chans")
			t.completeChans.Store(uuid.Must(uuid.NewV7()).String(), completeChan)
			//completeChans[uuid.Must(uuid.NewV7()).String()] = completeChan
		case trans := <-finishedTransactionChan:
			if t.currentTransaction < trans {
				t.currentTransaction = trans
			} else {
				log.Crit("Seems that the new transaction was not newer than the previous one. ", fmt.Sprintf("%d < %d", t.currentTransaction, trans))
			}
			t.completeChans.Range(func(id string, completeChan transactionCheck) bool {
				log.Debug(trans, completeChan.transaction)
				if trans < completeChan.transaction {
					return true
				}
				completeChan.completeChan <- struct{}{}
				t.completeChans.Delete(id)
				return true
			})
		default:
			upToDate = true
		}
	}
}

func (t *transaction[DT]) SetAndWait(e event.StoreEvent) (err error) {
	transaction, err := t.stream.Store(e, t.keyProvider)
	if err != nil {
		return
	}
	completeChan := make(chan struct{})
	defer close(completeChan)
	t.newTransactionChan <- transactionCheck{
		transaction:  transaction,
		completeChan: completeChan,
	}
	log.Debug("Set and wait waiting")
	<-completeChan
	return
}
