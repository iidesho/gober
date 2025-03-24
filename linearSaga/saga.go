package saga

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	consensus "github.com/iidesho/gober/consensus/contenious"
	"github.com/iidesho/gober/crypto"
	"github.com/iidesho/gober/discovery/local"
	"github.com/iidesho/gober/itr"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/consumer"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
	gsync "github.com/iidesho/gober/sync"
	"github.com/iidesho/gober/webserver"
)

const MESSAGE_BUFFER_SIZE = 1024

type Saga[BT, T any] interface {
	ExecuteFirst(T) (uuid.UUID, <-chan error, error)
	Status(uuid.UUID) (State, error)
	Close()
}

type executor[BT any, T bcts.ReadWriter[BT]] struct {
	ctx      context.Context
	es       consumer.Consumer[sagaValue[BT, T], *sagaValue[BT, T]]
	statuses gsync.Map[uuid.UUID, status]
	errors   gsync.Map[uuid.UUID, chan error]
	provider stream.CryptoKeyProvider
	failed   chan<- uuid.UUID
	close    context.CancelFunc
	sagaName string
	version  string
	story    Story[BT, T]
	tasks    []status
	taskLock sync.Mutex
}

func Init[BT any, T bcts.ReadWriter[BT]](
	pers stream.Stream,
	serv webserver.Server,
	dataTypeVersion, name string,
	story Story[BT, T],
	p stream.CryptoKeyProvider,
	workers int,
	ctx context.Context,
) (*executor[BT, T], error) {
	if len(story.Actions) <= 1 {
		err := ErrNotEnoughActions
		return nil, err
	}
	ctxTask, cancel := context.WithCancel(ctx)
	token := gsync.NewObj[string]()
	token.Set("someTestToken")
	// cons, err := consensus.Init(3134, token, local.New())
	dataTypeName := name + "_saga"
	es, err := consumer.New[sagaValue[BT, T]](pers, p, ctxTask)
	if err != nil {
		cancel()
		return nil, err
	}
	events, err := es.Stream(
		event.AllTypes(),
		store.STREAM_START,
		stream.ReadDataType(dataTypeName),
		ctx,
	)
	if err != nil {
		cancel()
		return nil, err
	}
	out := &executor[BT, T]{
		sagaName: dataTypeName,
		version:  dataTypeVersion,
		taskLock: sync.Mutex{},
		story:    story,
		statuses: gsync.NewMap[uuid.UUID, status](),
		errors:   gsync.NewMap[uuid.UUID, chan error](),
		es:       es,
		close:    cancel,
		ctx:      ctxTask,
	}
	discovery := local.New()
	for actI, action := range story.Actions {
		cons, aborted, approved, err := consensus.New(
			serv,
			token,
			discovery,
			fmt.Sprintf("saga_%s_%s", name, action.Id),
			ctxTask,
		)
		if err != nil {
			return nil, err
		}
		story.Actions[actI].cons = cons
		story.Actions[actI].aborted = aborted
		story.Actions[actI].approved = approved
	}
	// Should probably move this out to an external function created by the user instead. For now adding a customizable worker pool size
	exec := make(chan event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]])
	for range workers {
		go func(events <-chan event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]]) {
			for e := range events {
				func() {
					startTime := time.Now()
					defer func() {
						r := recover()
						if r != nil {
							log.WithError(
								fmt.Errorf("recoverd: %v, stack: %s", r, string(debug.Stack())),
							).Error("panic while executing")
							if errChan, ok := out.errors.Get(e.Data.status.id); ok {
								errChan <- fmt.Errorf("panic while executing, recoverd: %v, stack: %s", r, string(debug.Stack()))
							} else {
								sbragi.WithError(err).Error("error channel not found")
							}
							id, err := uuid.NewV7()
							sbragi.WithError(err).Fatal("could not generage UUID")
							sbragi.WithError(out.writeEvent(sagaValue[BT, T]{
								v: e.Data.v,
								status: status{
									stepDone:  e.Data.status.stepDone,
									retryFrom: "",
									duration:  time.Since(startTime),
									state:     StatePaniced,
									id:        e.Data.status.id,
									consID:    consensus.ConsID(id),
								},
							})).Error("writing panic event", "id", e.Data.status.id.String())
							return
						}
					}()
					log.Debug("selected task", "id", e.Data.status.id, "event", e)
					actionI := findStep(story.Actions, e.Data.status.stepDone)
					if actionI >= len(story.Actions) {
						log.Fatal(
							"this should never happen...",
							"saga",
							name,
							"actionLen",
							len(story.Actions),
							"gotI",
							actionI,
						)
						return
					}
					if actionI >= 0 && // Falesafing the -1 case
						(e.Data.status.state == StateRetryable ||
							e.Data.status.state == StateFailed) { // story.Actions[actionI].Id {
						state := StateSuccess
						err := story.Actions[actionI].Handler.Reduce(e.Data.v)
						if log. // Should not escalate
							WithError(err).
							Warning("there was an error while reducing saga part") {
							if errChan, ok := out.errors.Get(e.Data.status.id); ok {
								errChan <- err
							} else {
								sbragi.WithError(err).Error("error channel not found")
							}
							state = StateFailed
						}
						// out.consensus.Abort(consensus.ConsID(e.Data.Status.id))
						story.Actions[actionI].cons.Completed(e.Data.status.consID)
						retryFrom := e.Data.status.retryFrom
						consID := e.Data.status.consID
						if state == StateSuccess {
							if e.Data.status.state == StateRetryable {
								state = StateRetryable
								if actionI == 0 ||
									story.Actions[actionI].Id == e.Data.status.retryFrom {
									retryFrom = ""
									state = StatePending
									id, err := uuid.NewV7()
									sbragi.WithError(err).Fatal("could not generage UUID")
									consID = consensus.ConsID(id)
								}
							} else {
								state = e.Data.status.state
							}
						}

						prevStepID := ""
						if actionI != 0 {
							prevStepID = story.Actions[actionI-1].Id
						}
						sbragi.WithError(out.writeEvent(sagaValue[BT, T]{
							v: e.Data.v,
							status: status{
								stepDone:  prevStepID,
								retryFrom: retryFrom,
								duration:  time.Since(startTime),
								state:     state,
								id:        e.Data.status.id,
								consID:    consID,
							},
						})).Error("writing failed event", "id", e.Data.status.id.String())
						return
					}
					actionI++
					// Ignoring this state for now
					sbragi.WithError(out.writeEvent(sagaValue[BT, T]{
						v: e.Data.v,
						status: status{
							stepDone:  e.Data.status.stepDone,
							retryFrom: e.Data.status.stepDone,
							state:     StateWorking,
							id:        e.Data.status.id,
							consID:    e.Data.status.consID,
						},
					})).Error("writing panic event", "id", e.Data.status.id.String())
					state := StateSuccess
					err := story.Actions[actionI].Handler.Execute(e.Data.v)
					if log. // Should not escalate
						WithError(err).
						Warning("there was an error while executing task. not finishing") {
						// out.consensus.Abort(consensus.ConsID(e.Data.Status.id))
						if errChan, ok := out.errors.Get(e.Data.status.id); ok {
							errChan <- err
						} else {
							sbragi.WithError(err).Error("error channel not found")
						}
						state := StateFailed
						retryFrom := ""
						stepDone := e.Data.status.stepDone
						var retryError retryableError
						if errors.As(err, &retryError) {
							retryFrom = retryError.from
							state = StateRetryable
							stepDone = story.Actions[actionI].Id
						}
						id, err := uuid.NewV7()
						sbragi.WithError(err).Fatal("could not generage UUID")
						sbragi.WithError(out.writeEvent(sagaValue[BT, T]{
							v: e.Data.v,
							status: status{
								stepDone:  stepDone,
								retryFrom: retryFrom,
								duration:  time.Since(startTime),
								state:     state,
								id:        e.Data.status.id,
								consID:    consensus.ConsID(id),
							},
						})).Error("writing failed event", "id", e.Data.status.id.String())
						return
					}
					if state != StateSuccess {
						sbragi.Fatal(
							"Invalid saga state, should be success",
							"state",
							state,
							"value",
							e.Data,
						)
					}
					sbragi.WithError(out.writeEvent(sagaValue[BT, T]{
						v: e.Data.v,
						status: status{
							stepDone: story.Actions[actionI].Id,
							duration: time.Since(startTime),
							state:    state,
							id:       e.Data.status.id,
							consID:   e.Data.status.consID, // consensus.ConsID(rid),
						},
					})).Error("writing panic event", "id", e.Data.status.id.String())
					log.Trace("executed task", "event", e)
				}()
			}
		}(exec)
	}

	go out.handler(events, exec)

	return out, nil
}

func (t *executor[BT, T]) handler(
	events <-chan event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]],
	execChan chan event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]],
) {
	for e := range events {
		log.Debug(
			"read event",
			"event",
			e,
			"data",
			e.Data,
			"id",
			e.Data.status.id,
			"state",
			e.Data.status.state,
			"step",
			findStep(t.story.Actions, e.Data.status.stepDone)+1,
		)
		e.Acc()
		var actionI int
		switch e.Data.status.state {
		case StatePending:
			fallthrough
		case StateSuccess:
			t.taskLock.Lock()
			i := itr.NewIterator(t.tasks).
				Enumerate().
				Contains(func(v status) bool { return v.id == e.Data.status.id })
			if i >= 0 {
				t.tasks[i] = e.Data.status
			} else {
				t.tasks = append(t.tasks, e.Data.status)
			}
			t.taskLock.Unlock()
			actionI = findStep(t.story.Actions, e.Data.status.stepDone) + 1
			log.Trace("success / pending found")
			if actionI >= len(t.story.Actions) {
				if errChan, ok := t.errors.Get(e.Data.status.id); ok {
					close(errChan)
					t.errors.Delete(e.Data.status.id)
				}
				log.Trace("saga is completed")
				continue
			}
		case StatePaniced:
			fallthrough
		case StateFailed:
			t.taskLock.Lock()
			i := itr.NewIterator(t.tasks).
				Enumerate().
				Contains(func(v status) bool { return v.id == e.Data.status.id })
			if i >= 0 {
				t.tasks[i] = e.Data.status
			} else {
				t.tasks = append(t.tasks, e.Data.status)
			}
			t.taskLock.Unlock()
			if e.Data.status.stepDone == "" {
				if errChan, ok := t.errors.Get(e.Data.status.id); ok {
					close(errChan)
					t.errors.Delete(e.Data.status.id)
				}
				log.Trace("rollback completed as there is no more completed steps")
				continue
			}
			actionI = findStep(t.story.Actions, e.Data.status.stepDone)
			if actionI >= len(t.story.Actions) {
				log.Fatal(
					"this should never happen...",
					"saga",
					t.sagaName,
					"actionLen",
					len(t.story.Actions),
					"gotI",
					actionI,
				)
				continue
			}
			/* this no longer works as we are setting the stageDone when failing as well
			if actionI-1 == len(t.story.Actions) {
				log.Fatal(
					"we should never roll back a successfull saga",
					"saga",
					t.sagaName,
					"actionLen",
					len(t.story.Actions),
					"gotI",
					actionI,
				)
				continue
			}
			*/
			// This is just temporary, it will change when Barry is done...
			log.Trace("failed / paniced found")
		case StateRetryable:
			t.taskLock.Lock()
			i := itr.NewIterator(t.tasks).
				Enumerate().
				Contains(func(v status) bool { return v.id == e.Data.status.id })
			if i >= 0 {
				t.tasks[i] = e.Data.status
			} else {
				t.tasks = append(t.tasks, e.Data.status)
			}
			t.taskLock.Unlock()
			if e.Data.status.stepDone == "" {
				// t.story.Actions[0].cons.Request(e.Data.status.consID)
				actionI = 0
				e.Data.status.state = StatePending
				break
				/*
					} else if t.story.Actions[actionI+1].Id == e.Data.status.retryFrom {
						// This is just temporary, it will change when Barry is done...
						t.story.Actions[actionI+1].cons.Request(
							consensus.ConsID(e.Data.status.consID),
						) // this might / will have issues as we are not aborting these while reverting
						e.Data.status.state = StatePending
				*/
			}
			actionI = findStep(t.story.Actions, e.Data.status.stepDone)
			if actionI >= len(t.story.Actions) {
				log.Fatal(
					"this should never happen...",
					"saga",
					t.sagaName,
					"actionLen",
					len(t.story.Actions),
					"gotI",
					actionI,
				)
				continue
			}
			/* this no longer works as we are setting the stageDone when failing as well
			if actionI == len(t.story.Actions)-1 {
				log.Fatal(
					"we should never roll back a successfull saga",
					"saga",
					t.sagaName,
					"actionLen",
					len(t.story.Actions),
					"gotI",
					actionI,
				)
				continue
			}
			*/
			log.Trace("retryable found")
		case StateWorking:
			t.taskLock.Lock()
			i := itr.NewIterator(t.tasks).
				Enumerate().
				Contains(func(v status) bool { return v.id == e.Data.status.id })
			if i >= 0 {
				t.tasks[i] = e.Data.status
			} else {
				t.tasks = append(t.tasks, e.Data.status)
			}
			t.taskLock.Unlock()
			log.Trace("working found")
			continue
		default:
			log.Fatal("Invalid state found", "state", e.Data.status.state, "status", e.Data.status)
			continue
		}
		// This is just temporary, it will change when Barry is done...
		t.story.Actions[actionI].cons.Request(e.Data.status.consID)
		log.Debug(
			"won event",
			"name",
			t.es.Name(),
			"id",
			e.Data.status.id,
		)
		func(e event.ReadEventWAcc[sagaValue[BT, T], *sagaValue[BT, T]]) {
			log.Trace("time to do work, writing to exec chan")
			select {
			case execChan <- e:
				log.Trace(
					"wrote to exec chan",
					"name",
					t.es.Name(),
					"id",
					e.Data.status.id,
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
	data *sagaValue[BT, T],
) (e event.Event[sagaValue[BT, T], *sagaValue[BT, T]]) {
	e = event.Event[sagaValue[BT, T], *sagaValue[BT, T]]{
		Type: eventType,
		Data: data,
		Metadata: event.Metadata{
			Version:  t.version,
			DataType: t.sagaName,
			Key:      crypto.SimpleHash(data.status.id.String()),
		},
	}
	return
}

func (t *executor[BT, T]) writeEvent(tt sagaValue[BT, T]) error {
	we := event.NewWriteEvent(t.event(event.Created, &tt))
	t.es.Write() <- we
	ws := <-we.Done()
	return ws.Error
}

func (t *executor[BT, T]) ExecuteFirst(
	dt T,
) (uuid.UUID, <-chan error, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, nil, err
	}
	errChan := make(chan error, MESSAGE_BUFFER_SIZE)
	t.errors.Set(id, errChan)
	return id, errChan, t.writeEvent(sagaValue[BT, T]{
		v: dt,
		status: status{
			state:  StatePending,
			id:     id,
			consID: consensus.ConsID(id),
		},
	})
}

func (t *executor[BT, T]) Status(id uuid.UUID) (State, error) {
	st := itr.NewIterator(t.tasks).
		Filter(func(v status) bool { return v.id == id }).First()
	if st.id == uuid.Nil {
		return StateInvalid, ErrExecutionNotFound
	}
	if st.state == StateInvalid {
		return StateInvalid, errors.New("saga status was invalid")
	}
	log.Trace("got state", "step", st.stepDone, "state", st.state, "status", st)
	if st.state == StateSuccess {
		i := findStep(t.story.Actions, st.stepDone) + 1
		if i >= len(t.story.Actions) {
			return StateSuccess, nil
		}
		return StatePending, nil
	}
	return st.state, nil
}

func (t *executor[BT, T]) Close() {
	t.close()
}

/*
func (t *executor[BT, T]) Tasks() (tasks []status) {
	t.taskLock.Lock()
	defer t.taskLock.Unlock()
	tasks = make([]status, len(t.tasks))
	copy(tasks, t.tasks)
	return
}
*/

func findStep[BT any, T bcts.ReadWriter[BT]](actions []Action[BT, T], id string) int {
	if id == "" {
		return -1
	}
	for i, a := range actions {
		if a.Id == id {
			return i
		}
	}
	return -1
}

func RetryableError(from string, err error) retryableError {
	return retryableError{
		err:  err,
		from: from,
	}
}

type retryableError struct {
	err  error
	from string
}

func (r retryableError) Error() string {
	return fmt.Sprintf("RetryableError[%s]: %v", r.from, r.err)
}

var (
	// ErrRetryable                    = errors.New("error occured but can be retried")
	ErrNotEnoughActions             = errors.New("a story need more than one arc")
	ErrExecutionNotFound            = errors.New("saga id is invalid")
	ErrPreconditionsNosagaValueet   = errors.New("preconfitions not met for action")
	ErrBaseArcNeedsExactlyOneAction = errors.New("base arc can only and needs one action")
)
