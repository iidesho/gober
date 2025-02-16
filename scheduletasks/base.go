package tasks

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"time"

	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/consensus"
	"github.com/iidesho/gober/stream/consumer/competing"

	"github.com/iidesho/gober/crypto"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
)

var log = sbragi.WithLocalScope(sbragi.LevelError)

type Tasks[BT, T any] interface {
	Create(string, time.Time, time.Duration, T) error
	Tasks() (tasks []TaskMetadata)
}

type scheduledtasks[BT any, T bcts.ReadWriter[BT]] struct {
	ctx      context.Context
	es       competing.Consumer[tm[BT, T], *tm[BT, T]]
	provider stream.CryptoKeyProvider
	dataType string
	version  string
	tasks    []TaskMetadata
	timeout  time.Duration
	taskLock sync.Mutex
}

const NoInterval time.Duration = 0

// TaskMetadata temp changed task to be the id that is used for strong and id seems to now only be used for events.
type TaskMetadata struct {
	After    time.Time     `json:"after"`
	Id       string        `json:"id"`
	Interval time.Duration `json:"interval"`
}

func (t TaskMetadata) WriteBytes(w io.Writer) error {
	err := bcts.WriteSmallString(w, t.Id)
	if err != nil {
		return err
	}
	err = bcts.WriteTime(w, t.After)
	if err != nil {
		return err
	}
	return bcts.WriteInt64(w, t.Interval)
}

func (t *TaskMetadata) ReadBytes(r io.Reader) error {
	err := bcts.ReadSmallString(r, &t.Id)
	if err != nil {
		return err
	}
	err = bcts.ReadTime(r, &t.After)
	if err != nil {
		return err
	}
	return bcts.ReadInt64(r, &t.Interval)
}

type tm[BT any, T bcts.ReadWriter[BT]] struct {
	Task     T
	ctx      context.Context
	cancel   context.CancelFunc
	Metadata TaskMetadata
}

func (t tm[BT, T]) WriteBytes(w io.Writer) error {
	err := t.Metadata.WriteBytes(w)
	if err != nil {
		return err
	}
	return t.Task.WriteBytes(w)
}

func (t *tm[BT, T]) ReadBytes(r io.Reader) error {
	err := t.Metadata.ReadBytes(r)
	if err != nil {
		return err
	}
	t.Task, err = bcts.ReadReader[BT, T](r)
	return err
}

const timeoutOffsett = time.Second * 5

func Init[BT any, T bcts.ReadWriter[BT]](
	s stream.Stream,
	consBuilder consensus.ConsBuilderFunc,
	dataTypeName, dataTypeVersion string,
	p stream.CryptoKeyProvider,
	execute func(T, context.Context) bool,
	timeout time.Duration,
	skipable bool,
	workers int,
	ctx context.Context,
) (ed Tasks[BT, T], err error) {
	dataTypeName = dataTypeName + "_task"
	t := scheduledtasks[BT, T]{
		dataType: dataTypeName,
		version:  dataTypeVersion,
		ctx:      ctx,
	}
	es, err := competing.New(
		s,
		consBuilder,
		p,
		store.STREAM_START,
		dataTypeName,
		func(v *tm[BT, T]) (to time.Duration) {
			defer func() {
				log.Info(
					"timeout calculated",
					"duration",
					to,
					"name",
					v.Metadata.Id,
					"type",
					dataTypeName,
				)
			}()
			if v.Metadata.Id == "" {
				return timeout
			}
			t.taskLock.Lock()
			i := contains(v.Metadata.Id, t.tasks)
			if i >= 0 {
				if v.Metadata.After.After(t.tasks[i].After) {
					t.tasks[i] = v.Metadata
				}
			} else {
				t.tasks = append(t.tasks, v.Metadata)
			}
			t.taskLock.Unlock()
			if v.Metadata.After.After(time.Now().Add(timeout + timeoutOffsett)) {
				return time.Until(v.Metadata.After) - time.Second
			}
			return timeout + timeoutOffsett
		},
		ctx,
	) // This 15 min timout might be a huge issue
	if err != nil {
		return
	}
	t.es = es

	// Should probably move this out to an external function created by the user instead. For now adding a customizable worker pool size
	exec := make(chan competing.ReadEventWAcc[tm[BT, T], *tm[BT, T]])
	for i := 0; i < workers; i++ {
		go func(events <-chan competing.ReadEventWAcc[tm[BT, T], *tm[BT, T]]) {
			for e := range events {
				func() {
					defer func() {
						r := recover()
						if r != nil {
							log.WithError(
								fmt.Errorf("recoverd: %v, stack: %s", r, string(debug.Stack())),
							).Error("panic while executing")
							return
						}
					}()
					log.Info("selected task", "event", e)
					// Should be fixed now; This tsk is the one from tasks not scheduled tasks, thus the id is not the one that is used to store with here.
					if !execute(e.Data.Task, e.CTX) {
						log.Warning("there was an error while executing task. not finishing")
						return
					}
					log.Trace("executed task", "event", e)
					// Since the context can have timedout and thus another one could have been selected. This will create duplecate and competing tasks.
					select {
					case <-e.CTX.Done():
						log.Warning(
							"context closed while executing task",
							"name",
							e.Data.Metadata.Id,
						)
						return
					default:
					}
					if e.Data.Metadata.Interval != NoInterval {
						log.Trace("creating next task")
						err = t.create(
							e.Data.Metadata.Id,
							e.Data.Metadata.After.Add(e.Data.Metadata.Interval),
							e.Data.Metadata.Interval,
							e.Data.Task,
						)
						if err != nil {
							log.WithError(err).
								Error("while creating next event for finished action in scheduled task with interval")
							return
						}
						log.Trace("created next task")
					}
					e.Acc(e.Data)
				}()
			}
		}(exec)
	}

	go t.handler(timeout, skipable, exec)
	go func() {
		for range t.es.Completed() {
		} // Discard all completed events
	}()

	ed = &t
	return
}

func (s *scheduledtasks[BT, T]) handler(
	timeout time.Duration,
	skipable bool,
	execChan chan competing.ReadEventWAcc[tm[BT, T], *tm[BT, T]],
) {
	for e := range s.es.Stream() {
		// Could be valuable to keep the task collection here
		log.Info(
			"won event",
			"name",
			s.es.Name(),
			"id",
			e.Data.Metadata.Id,
			"skippable",
			skipable,
			"interval",
			e.Data.Metadata.Interval,
			"after",
			e.Data.Metadata.After,
			"before_now",
			time.Now().After(e.Data.Metadata.After.Add(e.Data.Metadata.Interval)),
		)
		if skipable && e.Data.Metadata.Interval != NoInterval &&
			time.Now().After(e.Data.Metadata.After.Add(e.Data.Metadata.Interval)) {
			log.Trace("skipping event, execution to late", "id", e.Data.Metadata.Id)
			// My issue is right here, it is not getting acepted as written
			err := s.create(
				e.Data.Metadata.Id,
				e.Data.Metadata.After.Add(e.Data.Metadata.Interval),
				e.Data.Metadata.Interval,
				e.Data.Task,
			)
			if err != nil {
				log.WithError(err).
					Error("while creating next event for finished action in scheduled task with interval")
				return
			}
			log.Trace("accing skipped event, execution to late", "id", e.Data.Metadata.Id)
			e.Acc(e.Data)
			log.Trace("acced skipped event, execution to late", "id", e.Data.Metadata.Id)
			continue
		}
		from, to := time.Now(), e.Data.Metadata.After
		waitTime := to.Sub(from)
		if waitTime > timeout {
			log.Trace(
				"no need to start waiting, timeout is before execution",
				"from",
				from,
				"to",
				to,
				"wait_time",
				waitTime,
				"timeout",
				timeout,
			)
			continue
		}
		go func(e competing.ReadEventWAcc[tm[BT, T], *tm[BT, T]]) {
			log.Info(
				"waiting until it is time to do work",
				"from",
				from,
				"to",
				to,
				"wait_time",
				waitTime,
			)
			select {
			case <-s.ctx.Done():
				log.Trace("service context timed out")
				return
			case <-e.CTX.Done():
				log.Trace("event context timed out")
				return
			case <-time.After(waitTime):
			}
			log.Info("time to do work, writing to exec chan")
			select {
			case execChan <- e:
				log.Info(
					"wrote to exec chan",
					"name",
					s.es.Name(),
					"id",
					e.Data.Metadata.Id,
					"interval",
					e.Data.Metadata.Interval,
				)
			case <-s.ctx.Done():
				log.Trace("service context timed out")
				return
			case <-e.CTX.Done():
				log.Trace("event context timed out")
				return
			}
		}(e)
	}
}

func (t *scheduledtasks[BT, T]) event(
	eventType event.Type,
	data *tm[BT, T],
) (e event.Event[tm[BT, T], *tm[BT, T]]) {
	e = event.Event[tm[BT, T], *tm[BT, T]]{
		Type: eventType,
		Data: data,
		Metadata: event.Metadata{
			Version:  t.version,
			DataType: t.dataType,
			Key:      crypto.SimpleHash(data.Metadata.Id),
		},
	}
	return
}

// To finish adding updatable tasks, should add task"name" and use that to store the task. Thus also checking if the that that is sent to delete is the one stored. Incase the next task comes before the delete-action for some reason.
func (t *scheduledtasks[BT, T]) Create(
	name string,
	a time.Time,
	i time.Duration,
	dt T,
) (err error) {
	return t.create(name, a, i, dt)
}

func (t *scheduledtasks[BT, T]) create(id string, a time.Time, i time.Duration, dt T) (err error) {
	we := event.NewWriteEvent(t.event(event.Created, &tm[BT, T]{
		Task: dt,
		Metadata: TaskMetadata{
			After:    a,
			Interval: i,
			Id:       id,
		},
	}))
	log.Trace("created event", "id", id)
	t.es.Write() <- we
	log.Trace("wrote event", "id", id)
	ws := <-we.Done()
	log.Trace("got event status", "id", id)
	return ws.Error
}

func (t *scheduledtasks[BT, T]) Tasks() (tasks []TaskMetadata) {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()
	tasks = make([]TaskMetadata, len(t.tasks))
	copy(tasks, t.tasks)
	return
}

func contains(id string, tasks []TaskMetadata) int {
	for i, t := range tasks {
		if t.Id != id {
			continue
		}
		return i
	}
	return -1
}
