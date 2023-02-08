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

type TaskData[DT any] struct {
	Id       uuid.UUID `json:"id"`
	Data     DT        `json:"data"`
	TimeOut  time.Time `json:"time_out,omitempty"`
	Selector string    `json:"selector"`
	Next     uuid.UUID `json:"next_id"`
}

func Init[DT any](s stream.Stream, dataTypeName, dataTypeVersion string, p stream.CryptoKeyProvider, ctx context.Context) (ed Tasks[DT], err error) {
	dataTypeName = dataTypeName + "_task"
	es, eventChan, err := competing.New[TaskData[DT]](s, p, store.STREAM_START, dataTypeName, time.Second*30, ctx) //Should not be start stream!
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
	return
}

func (t *tasks[DT]) Finish(id uuid.UUID) (err error) {
	taskAny, ok := t.data.Load(id.String())
	if !ok {
		err = fmt.Errorf("unfinishable as it does not exist, %s", id)
		return
	}
	taskAny.Acc()
	return
}

func (t *tasks[DT]) Create(id uuid.UUID, dt DT) (err error) {
	e := consumer.Event[TaskData[DT]]{
		Type: event.Create,
		Data: TaskData[DT]{
			Id:       id,
			Data:     dt,
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
