package saga

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	consensus "github.com/iidesho/gober/consensus/contenious"
	"github.com/iidesho/gober/discovery/local"
	"github.com/iidesho/gober/lte"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/sync"
	"github.com/iidesho/gober/webserver"
	// jsoniter "github.com/json-iterator/go"
)

var log = sbragi.WithLocalScope(sbragi.LevelDebug)

type Saga[BT any, T bcts.ReadWriter[BT]] interface {
	ExecuteFirst(T) (uuid.UUID, error)
	Status(uuid.UUID) (State, error)
	Close()
}

type transactionCheck struct {
	completeChan chan struct{}
	transaction  uint64
}

type saga[BT any, T bcts.ReadWriter[BT]] struct {
	close  func()
	states *sync.MapBCTS[states, *states]
	conses sync.Map[uuid.UUID, sagaValue[BT, T]]
	story  Story[BT, T]
}

type State uint8

const (
	StateInvalid State = iota
	StatePending
	StateWorking
	StateSuccess
	StateRetryable
	StateFailed
)

func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateWorking:
		return "working"
	case StateSuccess:
		return "success"
	case StateFailed:
		return "failed"
	default:
		return "invalid"
	}
}

func (s State) WriteBytes(w io.Writer) error {
	log.Info("writing", "s", s)
	return bcts.WriteUInt8(w, s)
}

func (s *State) ReadBytes(r io.Reader) error {
	return bcts.ReadUInt8(r, s)
}

type states []State

func (s states) WriteBytes(w io.Writer) error {
	err := bcts.WriteUInt32(w, uint32(len(s)))
	if err != nil {
		return err
	}
	for _, s := range s {
		err = s.WriteBytes(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *states) ReadBytes(r io.Reader) error {
	var l uint32
	err := bcts.ReadUInt32(r, &l)
	if err != nil {
		return err
	}
	*s = make(states, l)
	for i := range l {
		err = (*s)[i].ReadBytes(r)
		if err != nil {
			return err
		}
	}
	return nil
}

type Handler[BT any, T bcts.ReadWriter[BT]] interface {
	Execute(T) (State, error)
	Reduce(T) (State, error)
}

type handlerBuilder[BT any, T bcts.ReadWriter[BT]] struct {
	execute func(T) (State, error)
	reduce  func(T) (State, error)
}

type DumHandler[BT any, T bcts.ReadWriter[BT]] struct {
	ExecuteFunc func(T) (State, error)
	ReduceFunc  func(T) (State, error)
}

type emptyHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Execute(execute func(v T) (State, error)) executeHandlerBuilder[BT, T]
}
type executeHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Reduce(reduce func(v T) (State, error)) executeReduceHandlerBuilder[BT, T]
}
type executeReduceHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Build() Handler[BT, T]
}

func NewHandlerBuilder[BT any, T bcts.ReadWriter[BT]]() emptyHandlerBuilder[BT, T] {
	return handlerBuilder[BT, T]{}
}

func (h handlerBuilder[BT, T]) Execute(
	execute func(v T) (State, error),
) executeHandlerBuilder[BT, T] {
	h.execute = execute
	return h
}

func (h handlerBuilder[BT, T]) Reduce(
	reduce func(v T) (State, error),
) executeReduceHandlerBuilder[BT, T] {
	h.reduce = reduce
	return h
}

func (h handlerBuilder[BT, T]) Build() Handler[BT, T] {
	return DumHandler[BT, T]{
		ExecuteFunc: h.execute,
		ReduceFunc:  h.reduce,
	}
}

func (h DumHandler[BT, T]) Execute(v T) (State, error) {
	return h.ExecuteFunc(v)
}

func (h DumHandler[BT, T]) Reduce(v T) (State, error) {
	return h.ReduceFunc(v)
}

func NewDumHandler[BT any, T bcts.ReadWriter[BT]](
	execute func(T) (State, error),
	reduce func(T) (State, error),
) Handler[BT, T] {
	return DumHandler[BT, T]{
		ExecuteFunc: execute,
		ReduceFunc:  reduce,
	}
}

type Action[BT any, T bcts.ReadWriter[BT]] struct {
	// task    tasks.Tasks[sagaValue[BT, T], *sagaValue[BT, T]]
	task lte.Executor[sagaValue[BT, T], *sagaValue[BT, T]]
	// failed  <-chan uuid.UUID
	Handler Handler[BT, T]
	/*
		Status  func(T) (State, error)
		Execute func(T) error
		Reduce  func(T) error
	*/
	Id      string `json:"id"`
	Type    string `json:"type"`
	Timeout time.Duration
}

type Story[BT any, T bcts.ReadWriter[BT]] struct {
	Name    string
	Actions []Action[BT, T]
}

/*
var executors []struct {
	id string
	e  Action
	t  reflect.Type
	v  reflect.Value
}
*/

func (a *Action[BT, T]) ReadBytes(r io.Reader) error {
	err := bcts.ReadSmallString(r, &a.Id)
	if err != nil {
		return err
	}
	err = bcts.ReadSmallString(r, &a.Type)
	if err != nil {
		return err
	}
	err = bcts.ReadInt64(r, &a.Timeout)
	if err != nil {
		return err
	}
	return nil
	// return jsoniter.NewDecoder(r).Decode(a)
}

func (a Action[BT, T]) WriteBytes(w io.Writer) error {
	err := bcts.WriteSmallString(w, a.Id)
	if err != nil {
		return err
	}
	err = bcts.WriteSmallString(w, a.Type)
	if err != nil {
		return err
	}
	err = bcts.WriteInt64(w, a.Timeout)
	if err != nil {
		return err
	}
	return nil
	// return jsoniter.NewEncoder(w).Encode(a)
}

/*
func (a *Action) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	var tmpHandler map[string]interface{}
	val := reflect.ValueOf(*a)
	typ := val.Type()
	//typ := reflect.TypeOf(*a) //Use this instead of val.Type?
	for i := 0; i < typ.NumField(); i++ {
		switch typ.Field(i).Name {
		case "Id":
			a.Id = m[typ.Field(i).Tag.Get("json")].(string)
		case "Type":
			a.Type = m[typ.Field(i).Tag.Get("json")].(string)
		case "Handler":
			tmpHandler = m[typ.Field(i).Tag.Get("json")].(map[string]interface{})
		}
	}
	for _, e := range executors { //I would like a way to not use this global version.
		if a.Type != e.t.String() {
			continue
		}
		for i := 0; i < e.t.NumField(); i++ {
			if v, ok := tmpHandler[e.t.Field(i).Tag.Get("json")]; ok {
				e.v.Field(i).Set(reflect.ValueOf(v))
			}
		}
		a.Handler = e.v.Interface().(Action).Handler
	}
	return nil
}
*/

type sagaValue[BT any, T bcts.ReadWriter[BT]] struct {
	V      T         `json:"v"`
	ID     uuid.UUID `json:"id"`
	ConsID uuid.UUID `json:"cons_id"`
}

func (s *sagaValue[BT, T]) ReadBytes(r io.Reader) error {
	err := bcts.ReadStaticBytes(r, s.ID[:])
	if err != nil {
		return err
	}
	err = bcts.ReadStaticBytes(r, s.ConsID[:])
	if err != nil {
		return err
	}
	s.V, err = bcts.ReadReader[BT, T](r)
	return err
}

func (s sagaValue[BT, T]) WriteBytes(w io.Writer) error {
	err := bcts.WriteStaticBytes(w, s.ID[:])
	if err != nil {
		return err
	}
	err = bcts.WriteStaticBytes(w, s.ConsID[:])
	if err != nil {
		return err
	}
	return s.V.WriteBytes(w)
}

func Init[BT any, T bcts.ReadWriter[BT]](
	pers stream.Stream,
	serv webserver.Server,
	dataTypeVersion, name string,
	story Story[BT, T],
	p stream.CryptoKeyProvider,
	ctx context.Context,
) (*saga[BT, T], error) {
	if len(story.Actions) <= 1 {
		err := ErrNotEnoughActions
		return nil, err
	}
	ctxTask, cancel := context.WithCancel(ctx)
	token := sync.NewObj[string]()
	token.Set("someTestToken")
	// cons, err := consensus.Init(3134, token, local.New())
	out := &saga[BT, T]{
		close:  cancel,
		states: sync.NewMapBCTS[states](),
		conses: sync.NewMap[uuid.UUID, sagaValue[BT, T]](),
	}
	for actI, action := range story.Actions {
		actionId := fmt.Sprintf("%s_%s", name, action.Id)
		// should be moved into LTE...
		cons, aborted, complete, err := consensus.New(
			serv,
			token,
			local.New(),
			actionId,
			ctxTask,
		)
		if err != nil {
			cancel()
			return nil, err
		}
		go func() {
			for range complete {
			}
		}()
		story.Actions[actI].task, err = lte.Init(
			pers,
			cons,
			// cons.AddTopic,
			actionId,
			dataTypeVersion,
			p,
			func(v *sagaValue[BT, T], ctx context.Context) error {
				select {
				case <-ctx.Done():
					return fmt.Errorf("context done")
				default:
				}
				log.Info(
					"executing",
					"aid",
					actionId,
					"id",
					action.Id,
					"sid",
					v.ID.String(),
					"v",
					*v.V,
				)
				statuses, ok := out.states.Get(bcts.TinyString(v.ID.String()))
				if !ok {
					sbragi.Fatal(
						"tried getting states for undefined saga, behaviour is undefined",
						"id",
						v.ID.String(),
					)
				}
				if (*statuses)[actI] == StateSuccess {
					return nil
				}
				/*
					s, err := action.Handler.Status(v.V)
					if log.WithError(err).
						Error("getting action status", "saga", name, "action", action.Id) {
						return err
					}
				*/
				status, err := action.Handler.Execute(v.V)
				if log.WithError(err).
					Debug("executing action", "saga", name, "action", action.Id) {
					if errors.Is(err, ErrRetryable) {
						// TODO: This shoulc contain exponential backoff
						log.Info("sleeping 15 seconds before retrying retryable saga task")
						time.Sleep(time.Second * 15)
						err = story.Actions[actI].task.Retry(v.ID, v)
						log.WithError(err).
							Debug("start next action", "saga", name, "action", action.Id)
					}
					return err
				}
				(*statuses)[actI] = status
				if status != StateSuccess {
					out.states.Set(bcts.TinyString(v.ID.String()), statuses)
					return fmt.Errorf("invalid status expected=success got=%s", status.String())
				}
				if len(story.Actions) <= actI+1 {
					out.states.Set(bcts.TinyString(v.ID.String()), statuses)
					return nil
				}
				(*statuses)[actI+1] = StatePending
				out.states.Set(bcts.TinyString(v.ID.String()), statuses)
				err = story.Actions[actI+1].task.Create(uuid.Must(uuid.NewV7()), v)
				log.WithError(err).
					Debug("start next action", "saga", name, "action", action.Id)
				return err
			},
			5,
			ctxTask,
		)
		if err != nil {
			cancel()
			return nil, err
		}
		go func() {
			for {
				select {
				case <-ctxTask.Done():
					return
				case id := <-aborted:
					if data, ok := out.conses.Get(id.UUID()); ok {
						log.Error("task aborted", "data", data) // notice
					}
				}
			}
		}()
	}
	/*
		for li, l := range story.Arcs {
			var actions tasks.Tasks[Action, *Action]
			actions, err = tasks.Init(
				pers,
				cons.AddTopic,
				fmt.Sprintf("%s_%d_%s", name, li, ""),
				dataTypeVersion,
				p,
				func(a *Action, ctx context.Context) bool {
					select {
					case <-ctx.Done():
						return false
					default:
					}
					log.Info("executing", "id", a.Id)
					for i := range story.Arcs[li].Actions {
						if story.Arcs[li].Actions[i].Id == a.Id {
							a = &story.Arcs[li].Actions[i]
							break
						}
					}
					s, err := a.Handler.Status()
					if log.WithError(err).Debug("getting action status", "saga", name, "action", a.Id) {
						return false
					}
					if s != StatePending {
						return false
					}
					err = a.Handler.Execute()
					return !log.WithError(err).Debug("executing action", "saga", name, "action", a.Id)
				},
				l.Timeout,
				false,
				5,
				ctxTask,
			)
			if err != nil {
				cancel()
				return
			}
			/*
				for ai, a := range l.Actions {
					ra := reflect.ValueOf(a)
					story.Arcs[li].Actions[ai].Type = ra.Type().String()
					executorExists := false
					for _, e := range executors { //I would like a way to not use this global version.
						if ra.Type() == e.v.Type() {
							executorExists = true
							break
						}
					}
					if !executorExists {
						executors = append(executors, struct {
							id string
							e  Action
							t  reflect.Type
							v  reflect.Value
						}{id: fmt.Sprintf("%s_%d_%s", name, li, a.Id), e: a, v: ra, t: ra.Type()})
					}
				}
		}
	*/
	//go cons.Run()
	out.story = story
	/*
		selectChan := make(chan struct{}, 0)
		taskChan := make(chan taskChanData, 0)
		go func() {
			for range selectChan {
				t, _, e := out.story.Arcs[0].actions.Select()
				if errors.Is(e, tasks.NothingToSelectError) {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				taskChan <- taskChanData{
					task: t,
					err:  e,
				}
			}
		}()
		go func() {
			defer close(selectChan)
			for {
				selectChan <- struct{}{}
				select {
				case <-ctx.Done():
					return
				case selected := <-taskChan:
					fmt.Println("Sel:", selected)
					selected.task.Data.Body.Execute()
				}
			}
		}()
	*/
	return out, nil
}

func (s *saga[BT, T]) Status(id uuid.UUID) (State, error) {
	st, ok := s.states.Get(bcts.TinyString(id.String()))
	if !ok {
		return StateInvalid, ErrExecutionNotFound
	}
	if (*st)[0] == StateInvalid {
		return StateInvalid, errors.New("first saga status was invalid")
	}
	for i, s := range *st {
		if s == StateSuccess {
			continue
		}
		if s == StateInvalid {
			return (*st)[i-1], nil
		}
		return s, nil
	}
	return StateSuccess, nil // This path only happens if all statuses are success
}

func (s *saga[BT, T]) ExecuteFirst(v T) (id uuid.UUID, err error) {
	id, err = uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	for _, ok := s.states.Get(bcts.TinyString(id.String())); ok; _, ok = s.states.Get(bcts.TinyString(id.String())) { // This is veryVeryUnlikely
		id, err = uuid.NewV7()
		if err != nil {
			return uuid.Nil, err
		}
	}
	st := make(states, len(s.story.Actions))
	st[0] = StatePending
	s.states.Set(bcts.TinyString(id.String()), &st)
	sv := sagaValue[BT, T]{
		ID: id,
		V:  v,
	}
	err = s.story.Actions[0].task.Create(id, &sv)
	/*
		b := s.story.Arcs[0]
		a := b.Actions[0]
		a.Handler = e
		err = b.actions.Create("first_saga_task", time.Now(), tasks.NoInterval, &a)
	*/if err != nil {
		return uuid.Nil, err
	}
	return id, nil
	/*
		for _, a := range story.Actions {
			ra := reflect.ValueOf(a)
			a.Type = ra.Type().String()
			err = s.actions.Create(a, t)
			if err != nil {
				return
			}
		}
	*/
}

func (s *saga[BT, T]) Close() {
	s.close()
}

var (
	ErrRetryable                    = errors.New("error occured but can be retried")
	ErrNotEnoughActions             = errors.New("a story need more than one arc")
	ErrExecutionNotFound            = errors.New("saga id is invalid")
	ErrPreconditionsNotMet          = errors.New("preconfitions not met for action")
	ErrBaseArcNeedsExactlyOneAction = errors.New("base arc can only and needs one action")
)
