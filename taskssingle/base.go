package tasks

import (
	"context"
	"fmt"
	log "github.com/cantara/bragi"
	"time"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/gofrs/uuid"
)

type Tasks[DT any] interface {
	Create(DT) error
	Add(uuid.UUID, DT) error
	Select() (TaskData[DT], error)
	Finish(uuid.UUID) error
}

type transactionCheck struct {
	transaction  uint64
	completeChan chan struct{}
}

type tasks[DT any] struct {
	name             string
	data             Map[TaskData[DT]]
	transactionChan  chan transactionCheck
	eventTypeName    string
	eventTypeVersion string
	timeout          time.Duration
	provider         stream.CryptoKeyProvider
	completeChans    map[string]transactionCheck
	ctx              context.Context
	es               stream.Stream
	ec               <-chan event.Event[TaskData[DT]]
}

type selectionStatus string

const (
	Created  selectionStatus = "created"
	Selected selectionStatus = "selected"
	Finished selectionStatus = "finished"
)

type TaskData[DT any] struct {
	Id       uuid.UUID       `json:"id"`
	Data     DT              `json:"data"`
	Status   selectionStatus `json:"status"`
	TimeOut  time.Time       `json:"time_out,omitempty"`
	Selector string          `json:"selector"`
	Next     uuid.UUID       `json:"next_id"`
}

func Init[DT any](s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, ctx context.Context) (ed *tasks[DT], err error) {
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	eventChan, err := stream.NewStream[TaskData[DT]](s, event.AllTypes(), store.STREAM_START, stream.ReadDataType(dataTypeName), p, ctx)
	if err != nil {
		return
	}
	ed = &tasks[DT]{
		name:             name.String(),
		data:             New[TaskData[DT]](),
		transactionChan:  make(chan transactionCheck, 100),
		eventTypeName:    dataTypeName,
		eventTypeVersion: dataTypeVersion,
		provider:         p,
		completeChans:    make(map[string]transactionCheck),
		ctx:              ctx,
		es:               s,
		ec:               eventChan,
	}

	ed.readStream()
	return
}

func (t *tasks[DT]) readStream() {
	var currentTransaction uint64
	upToDate := false
	for !upToDate {
		time.Sleep(time.Millisecond)
		select {
		case <-t.ctx.Done():
			return
		case e := <-t.ec:
			if e.Transaction > currentTransaction {
				currentTransaction = e.Transaction
			}
			if e.Type == event.Delete {
				t.data.Delete(e.Data.Id.String())
				continue
			}
			t.data.Store(e.Data.Id.String(), e.Data)
		default:
			upToDate = true
		}
	}
	t.verifyWrite(currentTransaction)
}

func (t *tasks[DT]) verifyWrite(currentTransaction uint64) {
	upToDate := false
	for !upToDate {
		time.Sleep(time.Millisecond)
		select {
		case <-t.ctx.Done():
			return
		case completeChan := <-t.transactionChan:
			if currentTransaction >= completeChan.transaction {
				completeChan.completeChan <- struct{}{}
				continue
			}
			t.completeChans[uuid.Must(uuid.NewV7()).String()] = completeChan
		default:
			upToDate = true
		}
	}
	for id, completeChan := range t.completeChans {
		if currentTransaction < completeChan.transaction {
			continue
		}
		completeChan.completeChan <- struct{}{}
		delete(t.completeChans, id)
	}
}

func (t *tasks[DT]) Select() (outDT TaskData[DT], err error) {
	now := time.Now()
	t.data.Range(func(k, v any) bool {
		task := v.(TaskData[DT])
		if task.Status == Finished {
			return true
		}
		if task.Status == Selected && task.TimeOut.After(now) {
			return true
		}

		task.Status = Selected
		task.TimeOut = time.Now().Add(5 * time.Minute)
		task.Selector = t.name
		//task.Id = task.Next
		var e event.StoreEvent
		e, err = t.event(task.Next, event.Update, task)
		if err != nil {
			return false
		}
		//e.Metadata.Event = ev.Metadata.Event
		//ev.Readable.Lock()
		err = t.setAndWait(e)
		//ev.Readable.Unlock()
		if err != nil {
			return false
		}
		taskAny, ok := t.data.Load(task.Id.String())
		if !ok {
			err = fmt.Errorf("unselectable as it does not exist, %s", k)
			return false
		}
		outTask := taskAny //.(TaskData[DT])
		if outTask.Selector != t.name {
			return true
		}
		log.Debug(outTask)
		outDT = outTask
		return false
	})
	if outDT.Id.IsNil() {
		err = NothingToSelectError
		return
	}
	return
}

func (t *tasks[DT]) event(id uuid.UUID, eventType event.Type, data TaskData[DT]) (e event.StoreEvent, err error) {
	data.Next = uuid.Must(uuid.NewV7())
	return event.NewBuilder[TaskData[DT]]().
		WithId(id).
		WithType(eventType).
		WithData(data).
		WithMetadata(event.Metadata{
			Version:  t.eventTypeVersion,
			DataType: t.eventTypeName,
			Key:      crypto.SimpleHash(id.String()),
		}).
		BuildStore()
}

func (t *tasks[DT]) Finish(id uuid.UUID) (err error) {
	taskAny, ok := t.data.Load(id.String())
	if !ok {
		err = fmt.Errorf("unfinishable as it does not exist, %s", id)
		return
	}
	task := taskAny //.(TaskData[DT])
	if task.Status != Selected {
		err = fmt.Errorf("unfinishable as it is not selected, %s: %v", id, task)
		return
	}
	if task.Selector != t.name {
		err = fmt.Errorf("unfinishable as someone else is the selector, %s: %v", id, task)
		return
	}
	if task.TimeOut.Before(time.Now()) {
		err = fmt.Errorf("unfinishable as it has timed out, %s: %v", id, task)
		return
	}
	task.Status = Finished

	e, err := t.event(task.Next, event.Delete, task)
	if err != nil {
		return
	}
	err = t.setAndWait(e)
	return
}

func (t *tasks[DT]) Create(dt DT) (err error) {
	id, err := uuid.NewV7()
	if err != nil {
		return
	}
	return t.Add(id, dt)
}

func (t *tasks[DT]) Add(id uuid.UUID, dt DT) (err error) {
	e, err := t.event(id, event.Create, TaskData[DT]{
		Id:       id,
		Data:     dt,
		Status:   Created,
		Selector: "",
	})
	if err != nil {
		return
	}
	//e.Metadata.Event = mt
	err = t.setAndWait(e)
	return
}

func (t *tasks[DT]) setAndWait(e event.StoreEvent) (err error) {
	//d, _ := json.MarshalIndent(e, "", "    ")
	//log.Printf("Stream write: \n%s\n", d)
	transaction, err := t.es.Store(e, t.provider)
	if err != nil {
		return
	}
	completeChan := make(chan struct{}, 1)
	defer close(completeChan)
	t.transactionChan <- transactionCheck{
		transaction:  transaction,
		completeChan: completeChan,
	}
	log.Debug("Set and wait waiting")
	t.readStream()
	<-completeChan
	//t.data.Range(func(key, value any) bool {
	//fmt.Println("Range print", key, value)
	//return true
	//})
	return
}

var NothingToSelectError = fmt.Errorf("nothing to select")
