package tasks

import (
	"context"
	"fmt"
	log "github.com/cantara/bragi"
	"sync"
	"time"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/gofrs/uuid"
)

type Tasks[DT, MT any] interface {
	Create(DT, MT) error
	Select() (TaskData[DT], MT, error)
	Finish(uuid.UUID) error
}

type transactionCheck struct {
	transaction  uint64
	completeChan chan struct{}
}

type tasks[DT, MT any] struct {
	name             string
	data             sync.Map
	transactionChan  chan transactionCheck
	eventTypeName    string
	eventTypeVersion string
	timeout          time.Duration
	provider         stream.CryptoKeyProvider
	es               stream.Stream[TaskData[DT], MT]
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

type dmd[DT, MT any] struct {
	Task     *TaskData[DT]
	Metadata *event.Metadata[MT]
	//Readable *sync.Mutex
}

func Init[DT, MT any](s stream.Stream[TaskData[DT], MT], dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, getKey func(dt DT) string, ctx context.Context) (ed *tasks[DT, MT], err error) {
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	ed = &tasks[DT, MT]{
		name:             name.String(),
		data:             sync.Map{},
		transactionChan:  make(chan transactionCheck),
		eventTypeName:    dataTypeName,
		eventTypeVersion: dataTypeVersion,
		provider:         p,
		es:               s,
	}
	eventChan, err := s.Stream(event.AllTypes(), store.STREAM_START, stream.ReadDataType[MT](dataTypeName), p, ctx)
	if err != nil {
		return
	}
	upToDate := false
	for !upToDate {
		time.Sleep(time.Millisecond)
		select {
		case <-ctx.Done():
			return
		case e := <-eventChan:
			if e.Type == event.Delete {
				ed.data.Delete(e.Data.Id)
				continue
			}
			ed.data.Store(e.Data.Id, dmd[DT, MT]{
				Task:     &e.Data,
				Metadata: &e.Metadata,
				//Readable: &sync.Mutex{},
			})
		default:
			upToDate = true
		}
	}

	transactionChan := make(chan uint64, 5)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-eventChan:
				//d, _ := json.MarshalIndent(e, "", "    ")
				//log.Printf("Stream read: \n%s\n", d)
				if e.Type == event.Delete {
					ed.data.Delete(e.Data.Id)
					transactionChan <- e.Transaction
					continue
				}
				ed.data.Store(e.Data.Id, dmd[DT, MT]{
					Task:     &e.Data,
					Metadata: &e.Metadata,
					//Readable: &sync.Mutex{},
				})
				transactionChan <- e.Transaction
			}
		}
	}()

	go func() {
		completeChans := make(map[string]transactionCheck)
		var currentTransaction uint64
		for {
			select {
			case <-ctx.Done():
				return
			case completeChan := <-ed.transactionChan:
				if currentTransaction >= completeChan.transaction {
					completeChan.completeChan <- struct{}{}
					continue
				}

				completeChans[uuid.Must(uuid.NewV7()).String()] = completeChan
			case transaction := <-transactionChan:
				if currentTransaction < transaction {
					currentTransaction = transaction
				}
				for id, completeChan := range completeChans {
					if transaction < completeChan.transaction {
						continue
					}
					completeChan.completeChan <- struct{}{}
					delete(completeChans, id)
				}
			}
		}
	}()
	/*
		ticker := time.NewTicker(500 * time.Millisecond)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					now := time.Now()
					ed.data.Range(func(k, v any) bool {
						ev := v.(dmd[DT, MT])
						task := ev.Task
						if task.Status != Selected {
							return true
						}
						if task.TimeOut.Before(now) {
							ev.Task.Status
							ed.data.Store()
						}
						return true
					})
				}
			}
		}()
	*/
	return
}

func (t *tasks[DT, MT]) Select() (outDT TaskData[DT], outMT MT, err error) {
	now := time.Now()
	t.data.Range(func(k, v any) bool {
		ev := v.(dmd[DT, MT])
		task := ev.Task
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
		var e event.Event[TaskData[DT], MT]
		e, err = t.event(task.Next, event.Update, *task)
		if err != nil {
			return false
		}
		e.Metadata = *ev.Metadata
		//ev.Readable.Lock()
		err = t.setAndWait(e)
		//ev.Readable.Unlock()
		if err != nil {
			return false
		}
		taskAny, ok := t.data.Load(task.Id)
		if !ok {
			err = fmt.Errorf("unselectable as it does not exist, %s", k)
			return false
		}
		outTask := taskAny.(dmd[DT, MT])
		if outTask.Task.Selector != t.name {
			return true
		}
		log.Println(outTask)
		outDT = *outTask.Task
		outMT = outTask.Metadata.Event
		return false
	})
	if outDT.Id.IsNil() {
		err = NothingToSelectError
		return
	}
	return
}

func (t *tasks[DT, MT]) event(id uuid.UUID, eventType event.Type, data TaskData[DT]) (e event.Event[TaskData[DT], MT], err error) {
	data.Next = uuid.Must(uuid.NewV7())
	return event.NewBuilder[TaskData[DT], MT]().
		WithId(id).
		WithType(eventType).
		WithData(data).
		WithMetadata(event.Metadata[MT]{
			Version:  t.eventTypeVersion,
			DataType: t.eventTypeName,
			Key:      crypto.SimpleHash(id.String()),
		}).
		Build()
}

func (t *tasks[DT, MT]) Finish(id uuid.UUID) (err error) {
	taskAny, ok := t.data.Load(id)
	if !ok {
		err = fmt.Errorf("unfinishable as it does not exist, %s", id)
		return
	}
	task := taskAny.(dmd[DT, MT]).Task
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

	e, err := t.event(task.Next, event.Delete, *task)
	if err != nil {
		return
	}
	err = t.setAndWait(e)
	return
}

func (t *tasks[DT, MT]) Create(dt DT, mt MT) (err error) {
	id, err := uuid.NewV7()
	if err != nil {
		return
	}
	e, err := t.event(id, event.Create, TaskData[DT]{
		Id:       id,
		Data:     dt,
		Status:   Created,
		Selector: t.name,
	})
	if err != nil {
		return
	}
	e.Metadata.Event = mt
	err = t.setAndWait(e)
	return
}

func (t *tasks[DT, MT]) setAndWait(e event.Event[TaskData[DT], MT]) (err error) {
	//d, _ := json.MarshalIndent(e, "", "    ")
	//log.Printf("Stream write: \n%s\n", d)
	transaction, err := t.es.Store(e, t.provider)
	if err != nil {
		return
	}
	completeChan := make(chan struct{})
	defer close(completeChan)
	t.transactionChan <- transactionCheck{
		transaction:  transaction,
		completeChan: completeChan,
	}
	log.Println("Set and wait waiting")
	<-completeChan
	//t.data.Range(func(key, value any) bool {
	//fmt.Println("Range print", key, value)
	//return true
	//})
	return
}

var NothingToSelectError = fmt.Errorf("nothing to select")
