package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/tasks"
	"sync"
	"time"

	"github.com/cantara/gober/crypto"
	"github.com/cantara/gober/store"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/event"
	"github.com/gofrs/uuid"
)

type Tasks[DT any] interface {
	Create(time.Time, time.Duration, DT) error
}

type transactionCheck struct {
	transaction  uint64
	completeChan chan struct{}
}

type scheduledtasks[DT any] struct {
	name             string
	data             sync.Map
	transactionChan  chan transactionCheck
	eventTypeName    string
	eventTypeVersion string
	timeout          time.Duration
	provider         stream.CryptoKeyProvider
	es               stream.Stream
}

type selectionStatus string

const (
	Created  selectionStatus = "created"
	Selected selectionStatus = "selected"
	Finished selectionStatus = "finished"
)

const NoInterval time.Duration = 0

type TaskMetadata struct {
	Id       uuid.UUID       `json:"id"`
	Next     uuid.UUID       `json:"next_id"`
	NextTask uuid.UUID       `json:"next_task_id"`
	After    time.Time       `json:"after"`
	Interval time.Duration   `json:"interval"`
	Status   selectionStatus `json:"status"`
}

type tm[DT any] struct {
	Task     DT
	Metadata TaskMetadata
}

func Init[DT any](s stream.Stream, t tasks.Tasks[DT], dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, execute func(DT) bool, ctx context.Context) (ed *scheduledtasks[DT], err error) {
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	ed = &scheduledtasks[DT]{
		name:             name.String(),
		data:             sync.Map{},
		transactionChan:  make(chan transactionCheck),
		eventTypeName:    dataTypeName,
		eventTypeVersion: dataTypeVersion,
		provider:         p,
		es:               s,
	}
	eventChan, err := stream.NewStream[tm[DT]](s, event.AllTypes(), store.STREAM_START,
		stream.ReadDataType(dataTypeName), p, ctx)
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
			id := e.Data.Metadata.Id
			if e.Type == event.Delete {
				ed.data.Delete(id)
				continue
			}
			ed.data.Store(id, e.Data)
		default:
			upToDate = true
		}
	}

	createdTasksChan := make(chan uuid.UUID, 10)
	go func() {
		for id := range createdTasksChan {
			taskAny, loaded := ed.data.Load(id)
			if !loaded {
				log.Warning("tried loading " + id.String() + " but failed. unable to operate on the new task.")
				continue
			}
			task := taskAny.(tm[DT])
			if task.Metadata.Status != Created {
				log.Info("task already in or passed the selection stage. " + id.String())
				continue
			}
			//This can create very many go routines, but it is a very simple solution.
			go func() {
				defer func() {
					err := recover()
					if err != nil {
						log.AddError(err.(error)).Error("panic recovered while creating scheduled task")
					}
				}()
				waitingFor := task.Metadata.After.Sub(time.Now())
				log.Debug("Waiting for ", waitingFor.Minutes(), " minutes")
				time.Sleep(waitingFor)
				err := t.Add(task.Metadata.Id, task.Task)
				if err != nil {
					log.AddError(err).Error("while creating scheduled task")
					return
				}
				nextid, err := uuid.NewV7()
				if err != nil {
					log.AddError(err).Crit("while creating uuid for next action in scheduled task")
					return
				}
				e, err := ed.event(task.Metadata.Next, event.Update, tm[DT]{
					Task: task.Task,
					Metadata: TaskMetadata{
						Id:       task.Metadata.Id,
						Next:     nextid,
						NextTask: task.Metadata.NextTask,
						Status:   Selected,
						After:    task.Metadata.After,
						Interval: task.Metadata.Interval,
					},
				})
				if err != nil {
					log.AddError(err).Error("while creating event for next action in scheduled task")
					return
				}
				err = ed.setAndWait(e)
				if err != nil {
					log.AddError(err).Error("while storing event for next action in scheduled task")
					return
				}
			}()
		}
	}()

	transactionChan := make(chan uint64, 5)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-eventChan:
				id := e.Data.Metadata.Id
				if e.Type == event.Delete {
					ed.data.Delete(id)
					transactionChan <- e.Position
					continue
				}
				ed.data.Store(id, e.Data)
				transactionChan <- e.Position
				if e.Data.Metadata.Status == Created {
					createdTasksChan <- id
				}
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

	go func() {
		for {
			tsk, err := t.Select()
			if err != nil {
				if errors.Is(err, tasks.NothingToSelectError) {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				log.AddError(err).Crit("while selection task")
				continue
			}
			go func() {
				defer func() {
					err := recover()
					if err != nil {
						log.AddError(err.(error)).Crit("panic while executing")
						return
					}
				}()
				log.Debug("selected task: ", tsk)
				taskAny, loaded := ed.data.Load(tsk.Id)
				if !loaded {
					log.Warning("tried loading " + tsk.Id.String() + " but failed. unable to execute task.")
					return
				}
				task := taskAny.(tm[DT])
				if task.Metadata.Status != Selected {
					log.Info("task in impossible stage when selected for execution. " + tsk.Id.String())
					return
				}
				if !execute(tsk.Data) {
					log.Error("there was an error while executing task. not finishing")
					return
				}
				log.Debug("executed task:", task)
				terr := t.Finish(tsk.Id)
				if terr != nil {
					log.AddError(terr).Crit("while finishing task")
				}

				if task.Metadata.Interval != NoInterval {
					err = ed.create(task.Metadata.NextTask, task.Metadata.After.Add(task.Metadata.Interval), task.Metadata.Interval, task.Task)
					if err != nil {
						log.AddError(err).Crit("while creating event for finished action in scheduled task")
						return
					}
				}

				e, err := ed.event(task.Metadata.Next, event.Delete, tm[DT]{
					Task: tsk.Data, // not sure if i want to use the one from the select or the base from scheduletask
					Metadata: TaskMetadata{
						Id:       task.Metadata.Id,
						NextTask: task.Metadata.NextTask,
						Status:   Finished,
						After:    task.Metadata.After,
						Interval: task.Metadata.Interval,
					},
				})
				if err != nil {
					log.AddError(err).Error("while creating event for finished action in scheduled task")
					return
				}
				err = ed.setAndWait(e)
				if err != nil {
					log.AddError(err).Error("while storing event for finished action in scheduled task")
					return
				}
			}()
		}
	}()
	return
}

func (t *scheduledtasks[DT]) event(id uuid.UUID, eventType event.Type, data tm[DT]) (e event.StoreEvent, err error) {
	data.Metadata.Next = uuid.Must(uuid.NewV7())
	return event.NewBuilder[tm[DT]]().
		WithId(id).
		WithType(eventType).
		WithData(data).
		WithMetadata(event.Metadata{
			Version:  t.eventTypeVersion,
			DataType: t.eventTypeName,
			Key:      crypto.SimpleHash(id.String()),
			Extra: map[string]any{
				"select_status": data.Metadata.Status,
			},
		}).
		BuildStore()
}

func (t *scheduledtasks[DT]) Create(a time.Time, i time.Duration, dt DT) (err error) {
	id, err := uuid.NewV7()
	if err != nil {
		return
	}
	return t.create(id, a, i, dt)
}

func (t *scheduledtasks[DT]) create(id uuid.UUID, a time.Time, i time.Duration, dt DT) (err error) {
	nextid, err := uuid.NewV7()
	if err != nil {
		return
	}
	nextTaskid, err := uuid.NewV7()
	if err != nil {
		return
	}
	e, err := t.event(id, event.Create, tm[DT]{
		Task: dt,
		Metadata: TaskMetadata{
			Id:       id,
			Next:     nextid,
			NextTask: nextTaskid,
			Status:   Created,
			After:    a,
			Interval: i,
		},
	})
	if err != nil {
		return
	}
	err = t.setAndWait(e)
	return
}

func (t *scheduledtasks[DT]) setAndWait(e event.StoreEvent) (err error) {
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
	return
}

func printJson(v any) {
	o, _ := json.MarshalIndent(v, "", "  ")
	log.Println(string(o))
}

var NothingToSelectError = fmt.Errorf("nothing to select")
