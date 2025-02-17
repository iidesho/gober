package lte

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"sync"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/consensus/contenious"
	"github.com/iidesho/gober/itr"
	"github.com/iidesho/gober/stream/consumer"
	"github.com/iidesho/gober/stream/event/store"

	"github.com/iidesho/gober/crypto"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event"
)

var log = sbragi.WithLocalScope(sbragi.LevelError)

type Executor[BT, T any] interface {
	Create(uuid.UUID, T) error
	Tasks() (tasks []status)
	Retry(uuid.UUID, T) error
}

type executor[BT any, T bcts.ReadWriter[BT]] struct {
	ctx       context.Context
	es        consumer.Consumer[tm[BT, T], *tm[BT, T]]
	provider  stream.CryptoKeyProvider
	consensus contenious.Consensus
	dataType  string
	version   string
	// statuses    eventmap.EventMap[status, *status]
	// failed chan<- uuid.UUID
	// failCount map[uuid.UUID]int16
	tasks    []status
	taskLock sync.Mutex
}

type (
	Status string
	status struct {
		status Status
		id     uuid.UUID
		// executor uuid.UUID
	}
)

const (
	StatusPending   Status = "pending"
	StatusWorking   Status = "working"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusPaniced   Status = "paniced"
)

func (s status) WriteBytes(w io.Writer) error {
	/*
		err := bcts.WriteStaticBytes(w, s.executor[:]) // bcts.WriteSmallString(w, s.executor)
		if err != nil {
			return err
		}
	*/
	err := bcts.WriteStaticBytes(w, s.id[:]) // bcts.WriteSmallString(w, s.executor)
	if err != nil {
		return err
	}
	err = bcts.WriteTinyString(w, s.status)
	if err != nil {
		return err
	}
	return nil
}

func (s *status) ReadBytes(r io.Reader) error {
	// err := bcts.ReadSmallString(r, &s.executor)
	err := bcts.ReadStaticBytes(r, s.id[:])
	if err != nil {
		return err
	}
	err = bcts.ReadTinyString(r, &s.status)
	if err != nil {
		return err
	}

	return nil
}

/*
// TaskMetadata temp changed task to be the id that is used for strong and id seems to now only be used for events.
type TaskMetadata struct {
	Id string `json:"id"`
}

func (t TaskMetadata) WriteBytes(w io.Writer) error {
	err := bcts.WriteSmallString(w, t.Id)
	if err != nil {
		return err
	}
	return nil
}

func (t *TaskMetadata) ReadBytes(r io.Reader) error {
	err := bcts.ReadSmallString(r, &t.Id)
	if err != nil {
		return err
	}
	return nil
}
*/

type tm[BT any, T bcts.ReadWriter[BT]] struct {
	Task   T
	ctx    context.Context
	cancel context.CancelFunc
	Status status // TaskMetadata
}

func (t tm[BT, T]) WriteBytes(w io.Writer) error {
	err := t.Status.WriteBytes(w)
	if err != nil {
		return err
	}
	return t.Task.WriteBytes(w)
}

func (t *tm[BT, T]) ReadBytes(r io.Reader) error {
	err := t.Status.ReadBytes(r)
	if err != nil {
		return err
	}
	t.Task = new(BT)
	err = t.Task.ReadBytes(r)
	// t.Task, err = bcts.ReadReader[BT, T](r)
	return err
}

func Init[BT any, T bcts.ReadWriter[BT]](
	s stream.Stream,
	consnsus contenious.Consensus,
	dataTypeName, dataTypeVersion string,
	p stream.CryptoKeyProvider,
	execute func(T, context.Context) error,
	workers int,
	ctx context.Context,
) (ed Executor[BT, T], err error) {
	/*
		executorID, err := uuid.NewV7()
		if err != nil {
			return nil, err
		}
		executorIDS := executorID.String()
	*/
	dataTypeName = dataTypeName + "_execution"
	t := executor[BT, T]{
		dataType:  dataTypeName,
		version:   dataTypeVersion,
		consensus: consnsus,
		taskLock:  sync.Mutex{},
		// failCount:   map[uuid.UUID]int16{},
		ctx: ctx,
	}
	es, err := consumer.New[tm[BT, T]](s, p, ctx)
	if err != nil {
		return
	}
	t.es = es
	events, err := es.Stream(
		event.AllTypes(),
		store.STREAM_START,
		stream.ReadDataType(dataTypeName),
		ctx,
	)
	if err != nil {
		return nil, err
	}

	/*
		t.statuses, err = eventmap.Init[status](s, dataTypeName, dataTypeVersion, p, ctx)
		if err != nil {
			return nil, err
		}
	*/

	// Should probably move this out to an external function created by the user instead. For now adding a customizable worker pool size
	exec := make(chan event.ReadEventWAcc[tm[BT, T], *tm[BT, T]])
	for i := 0; i < workers; i++ {
		go func(events <-chan event.ReadEventWAcc[tm[BT, T], *tm[BT, T]]) {
			for e := range events {
				func() {
					defer func() {
						r := recover()
						if r != nil {
							log.WithError(
								fmt.Errorf("recoverd: %v, stack: %s", r, string(debug.Stack())),
							).Error("panic while executing")
							sbragi.WithError(t.writeEvent(tm[BT, T]{
								Task: e.Data.Task,
								Status: status{
									id:     e.Data.Status.id,
									status: StatusPaniced,
								},
							})).Error("writing panic event", "id", e.Data.Status.id.String())
							/*
								t.statuses.Set(e.Data.Status.Id, &status{
									executor: executorIDS,
									status:   StatusPaniced,
								})
							*/
							return
						}
					}()
					log.Info("selected task", "id", e.Data.Status.id, "event", e)
					// Should be fixed now; This tsk is the one from tasks not scheduled tasks, thus the id is not the one that is used to store with here.
					sbragi.WithError(t.writeEvent(tm[BT, T]{
						Task: e.Data.Task,
						Status: status{
							id:     e.Data.Status.id,
							status: StatusWorking,
						},
					})).Error("writing panic event", "id", e.Data.Status.id.String())
					/*
						t.statuses.Set(e.Data.Status.Id, &status{
							executor: executorIDS,
							status:   StatusWorking,
						})
					*/
					err := execute(e.Data.Task, e.CTX)
					if log. // Should not escalate
						WithError(err).
						Warning("there was an error while executing task. not finishing") {
						t.consensus.Abort(contenious.ConsID(e.Data.Status.id))
						sbragi.WithError(t.writeEvent(tm[BT, T]{
							Task: e.Data.Task,
							Status: status{
								id:     e.Data.Status.id,
								status: StatusFailed,
							},
						})).Error("writing failed event", "id", e.Data.Status.id.String())
						/*
							t.statuses.Set(e.Data.Status.Id, &status{
								executor: executorIDS,
								status:   StatusFailed,
							})
						*/
						/*
							if t.failed != nil {
								t.failed <- e.Data.Status.id
							}
						*/
						return
					}
					sbragi.WithError(t.writeEvent(tm[BT, T]{
						Task: e.Data.Task,
						Status: status{
							id:     e.Data.Status.id,
							status: StatusCompleted,
						},
					})).Error("writing panic event", "id", e.Data.Status.id.String())
					/*
						t.statuses.Set(e.Data.Status.Id, &status{
							executor: executorIDS,
							status:   StatusCompleted,
						})
					*/
					log.Trace("executed task", "event", e)
				}()
			}
		}(exec)
	}
	go t.handler(events, exec)

	ed = &t
	return
}

func (t *executor[BT, T]) handler(
	events <-chan event.ReadEventWAcc[tm[BT, T], *tm[BT, T]],
	execChan chan event.ReadEventWAcc[tm[BT, T], *tm[BT, T]],
) {
	for e := range events {
		e.Acc()
		switch e.Data.Status.status {
		case StatusCompleted:
			t.taskLock.Lock()
			i := itr.NewIterator(t.tasks).
				Enumerate().
				Contains(func(v status) bool { return v.id == e.Data.Status.id })
			if i >= 0 {
				t.tasks[i].status = StatusCompleted
			} else {
				t.tasks = append(t.tasks, e.Data.Status)
			}
			t.taskLock.Unlock()
			continue
		case StatusPaniced:
			fallthrough
		case StatusFailed:
			t.taskLock.Lock()
			i := itr.NewIterator(t.tasks).
				Enumerate().
				Contains(func(v status) bool { return v.id == e.Data.Status.id })
			if i >= 0 {
				t.tasks[i].status = StatusFailed
			} else {
				t.tasks = append(t.tasks, e.Data.Status)
			}
			/*
				if t.failed != nil {
					t.failed <- e.Data.Status.id
				}
				/*
					t.failCount[e.Data.Status.id]++
					if t.failCount[e.Data.Status.id] > 10 { // Should be configured
						t.taskLock.Unlock()
						continue
					}
					t.taskLock.Unlock()
			*/
		case StatusWorking:
			t.taskLock.Lock()
			i := itr.NewIterator(t.tasks).
				Enumerate().
				Contains(func(v status) bool { return v.id == e.Data.Status.id })
			if i >= 0 {
				t.tasks[i].status = StatusWorking
			} else {
				t.tasks = append(t.tasks, e.Data.Status)
			}
			t.taskLock.Unlock()
			continue
		}
		/*
			if st, err := s.statuses.Get(e.Data.Metadata.Id); err == nil && st.status != StatusPending {
				continue
			}
		*/
		// Could be valuable to keep the task collection here
		//This is just temporary, it will change when Barry is done...
		t.consensus.Request(contenious.ConsID(e.Data.Status.id))
		log.Info(
			"won event",
			"name",
			t.es.Name(),
			"id",
			e.Data.Status.id,
		)
		go func(e event.ReadEventWAcc[tm[BT, T], *tm[BT, T]]) {
			log.Info("time to do work, writing to exec chan")
			select {
			case execChan <- e:
				log.Info(
					"wrote to exec chan",
					"name",
					t.es.Name(),
					"id",
					e.Data.Status.id,
				)
			case <-t.ctx.Done():
				log.Trace("service context timed out")
				return
			case <-e.CTX.Done():
				log.Trace("event context timed out")
				return
			}
		}(e)
	}
}

func (t *executor[BT, T]) event(
	eventType event.Type,
	data *tm[BT, T],
) (e event.Event[tm[BT, T], *tm[BT, T]]) {
	e = event.Event[tm[BT, T], *tm[BT, T]]{
		Type: eventType,
		Data: data,
		Metadata: event.Metadata{
			Version:  t.version,
			DataType: t.dataType,
			Key:      crypto.SimpleHash(data.Status.id.String()),
		},
	}
	return
}

func (t *executor[BT, T]) writeEvent(tt tm[BT, T]) error {
	we := event.NewWriteEvent(t.event(event.Created, &tt))
	// log.Trace("created event", "id", id)
	t.es.Write() <- we
	// log.Trace("wrote event", "id", id)
	ws := <-we.Done()
	// log.Trace("got event status", "id", id)
	return ws.Error
}

// To finish adding updatable tasks, should add task"name" and use that to store the task. Thus also checking if the that that is sent to delete is the one stored. Incase the next task comes before the delete-action for some reason.
func (t *executor[BT, T]) Create(
	id uuid.UUID,
	dt T,
) (err error) {
	return t.create(id, dt)
}

// USE consBuilder and watch aborted / failed tasks...
// NOTE to self

func (t *executor[BT, T]) create(id uuid.UUID, dt T) (err error) {
	return t.writeEvent(tm[BT, T]{
		Task: dt,
		Status: status{
			id:     id,
			status: StatusPending,
		},
	})
	/*
		we := event.NewWriteEvent(t.event(event.Created, &tm[BT, T]{
			Task: dt,
			Status: status{
				id:     id,
				status: StatusPending,
			},
		}))
		log.Trace("created event", "id", id)
		t.es.Write() <- we
		log.Trace("wrote event", "id", id)
		ws := <-we.Done()
		log.Trace("got event status", "id", id)
		return ws.Error
	*/
}

func (t *executor[BT, T]) Retry(
	id uuid.UUID,
	dt T,
) (err error) {
	return t.create(id, dt)
}

func (t *executor[BT, T]) Tasks() (tasks []status) {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()
	tasks = make([]status, len(t.tasks))
	copy(tasks, t.tasks)
	return
}
