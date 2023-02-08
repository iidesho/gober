package tasks

import (
	"context"
	"fmt"
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

type scheduledtasks[DT any] struct {
	eventTypeName    string
	eventTypeVersion string
	timeout          time.Duration
	provider         stream.CryptoKeyProvider
	ctx              context.Context
	es               consumer.Consumer[tm[DT]]
	ec               <-chan consumer.ReadEvent[tm[DT]]
}

const NoInterval time.Duration = 0

// TaskMetadata temp changed task to be the id that is used for strong and id seems to now only be used for events.
type TaskMetadata struct {
	Id       uuid.UUID     `json:"id"`
	After    time.Time     `json:"after"`
	Interval time.Duration `json:"interval"`
}

type tm[DT any] struct {
	Task     DT
	Metadata TaskMetadata
	ctx      context.Context
	cancel   context.CancelFunc
}

func Init[DT any](s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, execute func(DT) bool, ctx context.Context) (ed Tasks[DT], err error) {
	es, eventChan, err := competing.New[tm[DT]](s, p, store.STREAM_START, stream.ReadDataType(dataTypeName), time.Minute*15, ctx)
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
	esTasks, taskEventChan, err := competing.New[DT](s, p, store.STREAM_START, stream.ReadDataType(dataTypeName+"_scheduled"), time.Second*30, ctx)
	if err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-t.ctx.Done():
				return
			case e := <-eventChan:
				log.Println("READ EVENT: ", e)
				go func() {
					log.Printf("waiting until it is time to do work, from %v to %v with waiting time of %v", e.Data.Metadata.After, time.Now(), e.Data.Metadata.After.Sub(time.Now()))
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
							DataType: t.eventTypeName + "_scheduled",
							Key:      crypto.SimpleHash(e.Data.Metadata.Id.String()),
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
							log.AddError(fmt.Errorf("%v", err)).Crit("panic while executing")
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

func (t *scheduledtasks[DT]) event(eventType event.Type, data tm[DT]) (e consumer.Event[tm[DT]], err error) {
	e = consumer.Event[tm[DT]]{
		Type: eventType,
		Data: data,
		Metadata: event.Metadata{
			Version:  t.eventTypeVersion,
			DataType: t.eventTypeName,
			Key:      crypto.SimpleHash(data.Metadata.Id.String()),
		},
	}
	return
}

// To finish adding updatable tasks, should add task"name" and use that to store the task. Thus also checking if the that that is sent to delete is the one stored. Incase the next task comes before the delete for some reason.
func (t *scheduledtasks[DT]) Create(a time.Time, i time.Duration, dt DT) (err error) {
	e, err := t.event(event.Create, tm[DT]{
		Task: dt,
		Metadata: TaskMetadata{
			After:    a,
			Interval: i,
			Id:       uuid.Must(uuid.NewV7()),
		},
	})
	if err != nil {
		return
	}
	_, err = t.es.Store(e)
	return
}
