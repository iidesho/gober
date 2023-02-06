package tasks

import (
	"context"
	"fmt"
	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/consumer/competing"
	"github.com/cantara/gober/stream/event"
	"sync"
	"time"

	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream"
	"github.com/gofrs/uuid"
)

type Tasks[DT any] interface {
	Create(uuid.UUID, DT) error
	Select() (TaskData[DT], error)
	Finish(uuid.UUID) error
}

type tasks[DT any] struct {
	name             string
	data             Map[consumer.ReadEvent[TaskData[DT]]]
	eventTypeName    string
	eventTypeVersion string
	timeout          time.Duration
	provider         stream.CryptoKeyProvider
	ctx              context.Context
	es               consumer.Consumer[TaskData[DT]]
	ec               <-chan consumer.ReadEvent[TaskData[DT]]
	selectLock       sync.Mutex
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

func Init[DT any](s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, ctx context.Context) (ed Tasks[DT], err error) {
	es, eventChan, err := competing.New[TaskData[DT]](s, p, store.STREAM_START, stream.ReadDataType(dataTypeName), ctx) //Should not be start stream!
	if err != nil {
		return
	}
	t := tasks[DT]{
		data:             New[consumer.ReadEvent[TaskData[DT]]](),
		eventTypeName:    dataTypeName,
		eventTypeVersion: dataTypeVersion,
		ctx:              ctx,
		es:               es,
		ec:               eventChan,
		selectLock:       sync.Mutex{},
	}

	ed = &t
	return
}

func (t *tasks[DT]) Select() (outDT TaskData[DT], err error) {
	select {
	case e := <-t.ec:
		t.data.Store(e.Data.Id.String(), e)
		outDT = e.Data
	case <-time.Tick(time.Second):
		err = NothingToSelectError
	}
	/*
		t.selectLock.Lock()
		defer t.selectLock.Unlock()
		now := time.Now()
		t.data.Range(func(k string, task TaskData[DT]) bool {
			if task.Status == Finished {
				return true
			}
			if task.Status == Selected && task.TimeOut.After(now) {
				return true
			}

			task.Status = Selected
			task.TimeOut = time.Now().Add(5 * time.Minute)
			task.Selector = t.name
			var e event.StoreEvent
			e, err = t.event(task.Next, event.Update, task)
			if err != nil {
				return false
			}
			err = t.esh.SetAndWait(e)
			if err != nil {
				return false
			}
			taskAny, ok := t.data.Load(task.Id.String())
			if !ok {
				err = fmt.Errorf("unselectable as it does not exist, %s", k)
				return false
			}
			outTask := taskAny
			if outTask.Selector != t.name {
				return true
			}
			outDT = outTask
			return false
		})
		if outDT.Id.IsNil() {
			err = NothingToSelectError
			return
		}
	*/
	return
}

/*
func (t *tasks[DT]) event(id uuid.UUID, eventType event.Type, data TaskData[DT]) (e st, err error) {
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
*/

func (t *tasks[DT]) Finish(id uuid.UUID) (err error) {
	taskAny, ok := t.data.Load(id.String())
	if !ok {
		err = fmt.Errorf("unfinishable as it does not exist, %s", id)
		return
	}
	taskAny.Acc()
	/*
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
		err = t.esh.SetAndWait(e)
	*/
	return
}

/* Deprecating oversimplified helper
func (t *tasks[DT]) Create(dt DT) (err error) {
	id, err := uuid.NewV7()
	if err != nil {
		return
	}
	return t.Add(id, dt)
}
*/

func (t *tasks[DT]) Create(id uuid.UUID, dt DT) (err error) {
	/*
		e, err := t.event(id, event.Create, TaskData[DT]{
			Id:       id,
			Data:     dt,
			Status:   Created,
			Selector: "",
		})
		if err != nil {
			return
		}
	*/
	//e.Metadata.Event = mt
	e := consumer.Event[TaskData[DT]]{
		Type: event.Create,
		Data: TaskData[DT]{
			Id:       id,
			Data:     dt,
			Status:   Created,
			Selector: "",
		},
		Metadata: event.Metadata{
			DataType: t.eventTypeName,
			Version:  t.eventTypeVersion,
			Key:      crypto.SimpleHash(id.String()),
		},
	}
	_, err = t.es.Store(e)
	return
}

var NothingToSelectError = fmt.Errorf("nothing to select")
