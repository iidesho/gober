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
	"github.com/iidesho/gober/consensus"
	"github.com/iidesho/gober/discovery/local"
	tasks "github.com/iidesho/gober/scheduletasks"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/sync"
	jsoniter "github.com/json-iterator/go"
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
	story  Story[BT, T]
	states *sync.Map[states, *states]
}

type State uint8

const (
	StateInvalid State = iota
	StatePending
	StateWorking
	StateSuccess
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
	Status(T) (State, error)
	Execute(T) error
	Reduce(T) error
}

type Action[BT any, T bcts.ReadWriter[BT]] struct {
	task    tasks.Tasks[sagaValue[BT, T], *sagaValue[BT, T]]
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
	return jsoniter.NewDecoder(r).Decode(a)
}

func (a Action[BT, T]) WriteBytes(w io.Writer) error {
	return jsoniter.NewEncoder(w).Encode(a)
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
	V  T         `json:"v"`
	ID uuid.UUID `json:"id"`
}

func (s *sagaValue[BT, T]) ReadBytes(r io.Reader) error {
	err := bcts.ReadStaticBytes(r, s.ID[:])
	if err != nil {
		return err
	}
	return s.V.ReadBytes(r)
}

func (s sagaValue[BT, T]) WriteBytes(w io.Writer) error {
	err := bcts.WriteStaticBytes(w, s.ID[:])
	if err != nil {
		return err
	}
	return s.V.WriteBytes(w)
}

func Init[BT any, T bcts.ReadWriter[BT]](
	pers stream.Stream,
	dataTypeVersion, name string,
	story Story[BT, T],
	p stream.CryptoKeyProvider,
	ctx context.Context,
) (out *saga[BT, T], err error) {
	if len(story.Actions) <= 1 {
		err = ErrNotEnoughActions
		return
	}
	ctxTask, cancel := context.WithCancel(ctx)
	token := "someTestToken"
	cons, err := consensus.Init(3134, token, local.New())
	if err != nil {
		cancel()
		return nil, err
	}
	out = &saga[BT, T]{
		close:  cancel,
		states: sync.NewMap[states, *states](),
	}
	for actI, action := range story.Actions {
		actionId := fmt.Sprintf("%s_%s", name, action.Id)
		story.Actions[actI].task, err = tasks.Init(
			pers,
			cons.AddTopic,
			actionId,
			dataTypeVersion,
			p,
			func(v *sagaValue[BT, T], ctx context.Context) bool {
				select {
				case <-ctx.Done():
					return false
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
					v.V,
				)
				statuses, ok := out.states.Get(bcts.TinyString(v.ID.String()))
				log.Info("got states", "ok", ok, "id", v.ID.String(), "statuses", statuses)
				if (*statuses)[actI] == StateSuccess {
					return true
				}
				s, err := action.Handler.Status(v.V)
				if log.WithError(err).
					Debug("getting action status", "saga", name, "action", action.Id) {
					return false
				}
				if s == StateSuccess {
					(*statuses)[actI] = StateSuccess
					out.states.Set(bcts.TinyString(v.ID.String()), statuses)
					return true
				}
				if s != StatePending {
					return false
				}
				err = action.Handler.Execute(v.V)
				if log.WithError(err).
					Debug("executing action", "saga", name, "action", action.Id) {
					return false
				}
				s, err = action.Handler.Status(v.V)
				if log.WithError(err).
					Debug("getting action status after execute", "saga", name, "action", action.Id) {
					return false
				}
				(*statuses)[actI] = s
				if s != StateSuccess {
					out.states.Set(bcts.TinyString(v.ID.String()), statuses)
					return false
				}
				if len(story.Actions) <= actI+1 {
					out.states.Set(bcts.TinyString(v.ID.String()), statuses)
					return true
				}
				(*statuses)[actI+1] = StatePending
				out.states.Set(bcts.TinyString(v.ID.String()), statuses)
				err = story.Actions[actI+1].task.Create(fmt.Sprintf(
					"%s_%s_%s",
					story.Name,
					story.Actions[actI+1].Id,
					uuid.Must(uuid.NewV7()).String(),
				),
					time.Now(),
					tasks.NoInterval,
					v)
				return !log.WithError(err).
					Debug("start next action", "saga", name, "action", action.Id)
			},
			action.Timeout,
			false,
			5,
			ctxTask,
		)
		if err != nil {
			cancel()
			return
		}

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
	go cons.Run()
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
	return
}

func (s *saga[BT, T]) Status(id uuid.UUID) (State, error) {
	st, ok := s.states.Get(bcts.TinyString(id.String()))
	if !ok {
		return StateInvalid, errors.New("saga id is invalid")
	}
	if (*st)[0] == StateInvalid {
		return StateInvalid, errors.New("first saga status was invalid")
	}
	log.Info("has valid status", "statuses", st, "id", id.String())
	for i, s := range *st {
		if s == StateSuccess {
			continue
		}
		if s == StateInvalid {
			return (*st)[i-1], nil
		}
		return s, nil
	}
	return StateSuccess, nil //This path only happens if all statuses are success
}
func (s *saga[BT, T]) ExecuteFirst(v T) (id uuid.UUID, err error) {
	id, err = uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	for _, ok := s.states.Get(bcts.TinyString(id.String())); ok; _, ok = s.states.Get(bcts.TinyString(id.String())) { //This is veryVeryUnlikely
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
	log.Info("executing first", "sv", sv, "states", st)
	err = s.story.Actions[0].task.Create(
		fmt.Sprintf(
			"%s_%s_%s",
			s.story.Name,
			s.story.Actions[0].Id,
			id.String(),
		),
		time.Now(),
		tasks.NoInterval,
		&sv,
	)
	/*
		b := s.story.Arcs[0]
		a := b.Actions[0]
		a.Handler = e
		err = b.actions.Create("first_saga_task", time.Now(), tasks.NoInterval, &a)
	*/
	if err != nil {
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

var ErrPreconditionsNotMet = errors.New("preconfitions not met for action")
var ErrNotEnoughActions = errors.New("a story need more than one arc")
var ErrBaseArcNeedsExactlyOneAction = errors.New("base arc can only and needs one action")
