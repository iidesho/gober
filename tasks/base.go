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

/*
Tasks
there is a issue within the implementation of the tasks. It is not consistent when running concurrent.
Temp fix is to use the new implementation found in taskssingle.
*/
type Tasks[DT any] interface {
	Create(DT) error
	Add(uuid.UUID, DT) error
	Select() (TaskData[DT], error)
	Finish(uuid.UUID) error
}

type tasks[DT any] struct {
	name             string
	data             Map[TaskData[DT]]
	eventTypeName    string
	eventTypeVersion string
	timeout          time.Duration
	provider         stream.CryptoKeyProvider
	ctx              context.Context
	es               stream.Stream
	ec               <-chan event.Event[TaskData[DT]]
	esh              stream.SetHelper
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
		eventTypeName:    dataTypeName,
		eventTypeVersion: dataTypeVersion,
		provider:         p,
		ctx:              ctx,
		es:               s,
		ec:               eventChan,
	}
	ed.esh = stream.InitSetHelper(func(e event.Event[TaskData[DT]]) {
		ed.data.Store(e.Data.Id.String(), e.Data)
	}, func(e event.Event[TaskData[DT]]) {
		ed.data.Delete(e.Data.Id.String())
	}, ed.es, ed.provider, ed.ec, ed.ctx)

	return
}

func (t *tasks[DT]) Select() (outDT TaskData[DT], err error) {
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
		//task.Id = task.Next
		var e event.StoreEvent
		e, err = t.event(task.Next, event.Update, task)
		if err != nil {
			return false
		}
		//e.Metadata.Event = ev.Metadata.Event
		//ev.Readable.Lock()
		err = t.esh.SetAndWait(e)
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
	err = t.esh.SetAndWait(e)
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
	err = t.esh.SetAndWait(e)
	return
}

var NothingToSelectError = fmt.Errorf("nothing to select")
