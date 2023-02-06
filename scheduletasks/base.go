package tasks

import (
	"context"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/consumer/competing"
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
	eventTypeName    string
	eventTypeVersion string
	timeout          time.Duration
	provider         stream.CryptoKeyProvider
	ctx              context.Context
	es               consumer.Consumer[tm[DT]]
	ec               <-chan consumer.ReadEvent[tm[DT]]
}

type selectionStatus string

const (
	Created  selectionStatus = "created"
	Selected selectionStatus = "selected"
	Finished selectionStatus = "finished"
)

const NoInterval time.Duration = 0

// TaskMetadata temp changed task to be the id that is used for strong and id seems to now only be used for events.
type TaskMetadata struct {
	Id       uuid.UUID       `json:"id"`
	Task     uuid.UUID       `json:"task"` //Needs to be added because both sheduledtask and task can run in the same stream.
	Next     uuid.UUID       `json:"next_id"`
	NextTask uuid.UUID       `json:"next_task_id"`
	After    time.Time       `json:"after"`
	Interval time.Duration   `json:"interval"`
	Status   selectionStatus `json:"status"`
}

type tm[DT any] struct {
	Task     DT
	Metadata TaskMetadata
	ctx      context.Context
	cancel   context.CancelFunc
}

func Init[DT any](s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, execute func(DT) bool, ctx context.Context) (ed Tasks[DT], err error) {
	es, eventChan, err := competing.New[tm[DT]](s, p, store.STREAM_START, stream.ReadDataType(dataTypeName), ctx)
	if err != nil {
		return
	}
	t := scheduledtasks[DT]{
		eventTypeName:    dataTypeName,
		eventTypeVersion: dataTypeVersion,
		ctx:              ctx,
		es:               es,
		ec:               eventChan,
	}
	esTasks, taskEventChan, err := competing.New[DT](s, p, store.STREAM_START, stream.ReadDataType(dataTypeName), ctx)
	//tsks, err := tasks.Init[tm[DT]](s, dataTypeName+"_scheduled", dataTypeVersion, p, ctx)
	if err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-t.ctx.Done():
				return
			case e := <-eventChan:
				go func() {
					select {
					case <-t.ctx.Done():
						return
					case <-e.CTX.Done():
						return
					case <-time.After(e.Data.Metadata.After.Sub(time.Now())):
					}
					_, err := esTasks.Store(consumer.Event[DT]{
						Data: e.Data.Task,
						Metadata: event.Metadata{
							Version:  t.eventTypeVersion,
							DataType: t.eventTypeName,
							Key:      crypto.SimpleHash(e.Data.Metadata.Task.String()),
						},
					})
					if err != nil {
						log.AddError(err).Error("while creating scheduled task")
						return
					}
					if e.Data.Metadata.Interval != NoInterval {
						err = t.Create(e.Data.Metadata.After.Add(e.Data.Metadata.Interval), e.Data.Metadata.Interval, e.Data.Task)
						if err != nil {
							log.AddError(err).Crit("while creating event for finished action in scheduled task")
							return
						}
					}
					e.Acc()
				}()
			}
		}
	}()

	go func() {
		for {
			select {
			case <-t.ctx.Done():
				return
			case e := <-taskEventChan:
				go func() {
					defer func() {
						err := recover()
						if err != nil {
							log.AddError(err.(error)).Crit("panic while executing")
							return
						}
					}()
					log.Debug("selected task: ", e)
					// Should be fixed now; This tsk is the one from tasks not scheduled tasks, thus the id is not the one that is used to store with here.
					if !execute(e.Data) {
						log.Error("there was an error while executing task. not finishing")
						return
					}
					log.Debug("executed task:", e)
					e.Acc()
				}()
			}
		}
	}()

	ed = &t
	return
}

func (t *scheduledtasks[DT]) event(id uuid.UUID, eventType event.Type, data tm[DT]) (e consumer.Event[tm[DT]], err error) {
	data.Metadata.Next = uuid.Must(uuid.NewV7())
	e = consumer.Event[tm[DT]]{
		Type: eventType,
		Data: data,
		Metadata: event.Metadata{
			Version:  t.eventTypeVersion,
			DataType: t.eventTypeName,
			Key:      crypto.SimpleHash(id.String()),
			Extra: map[string]any{
				"select_status": data.Metadata.Status,
			},
		},
	}
	return
}

// To finish adding updatable tasks, should add task"name" and use that to store the task. Thus also checking if the that that is sent to delete is the one stored. Incase the next task comes before the delete for some reason.
func (t *scheduledtasks[DT]) Create(a time.Time, i time.Duration, dt DT) (err error) {
	id, err := uuid.NewV7()
	if err != nil {
		return
	}
	return t.create(id, a, i, dt)
}

func (t *scheduledtasks[DT]) create(id uuid.UUID, a time.Time, i time.Duration, dt DT) (err error) {
	taskId, err := uuid.NewV7()
	if err != nil {
		return
	}
	nextId, err := uuid.NewV7()
	if err != nil {
		return
	}
	nextTaskId, err := uuid.NewV7()
	if err != nil {
		return
	}
	e, err := t.event(id, event.Create, tm[DT]{
		Task: dt,
		Metadata: TaskMetadata{
			Id:       id,
			Task:     taskId,
			Next:     nextId,
			NextTask: nextTaskId,
			Status:   Created,
			After:    a,
			Interval: i,
		},
	})
	if err != nil {
		return
	}
	_, err = t.es.Store(e)
	return
}
