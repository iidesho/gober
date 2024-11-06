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
	jsoniter "github.com/json-iterator/go"
)

var log = sbragi.WithLocalScope(sbragi.LevelDebug)

type Saga interface {
	ExecuteFirst() error
	Close()
}

type transactionCheck struct {
	completeChan chan struct{}
	transaction  uint64
}

type saga[BT any, T bcts.ReadWriter[BT]] struct {
	close func()
	story Story[BT, T]
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

type Handler interface {
	Status() (State, error)
	Execute() error
	Reduce() error
}

type Action[BT any, T bcts.ReadWriter[BT]] struct {
	Status  func(T) (State, error)
	Execute func(T) error
	Reduce  func(T) error
	//Handler Handler `json:"handler"`
	Id      string `json:"id"`
	Type    string `json:"type"`
	task    tasks.Tasks[BT, T]
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
	for actI, action := range story.Actions {
		actionId := fmt.Sprintf("%s_%s", name, action.Id)
		story.Actions[actI].task, err = tasks.Init(
			pers,
			cons.AddTopic,
			actionId,
			dataTypeVersion,
			p,
			func(v T, ctx context.Context) bool {
				select {
				case <-ctx.Done():
					return false
				default:
				}
				log.Info("executing", "aid", actionId, "id", action.Id)
				s, err := action.Status(v)
				if log.WithError(err).
					Debug("getting action status", "saga", name, "action", action.Id) {
					return false
				}
				if s == StateSuccess {
					return true
				}
				if s != StatePending {
					return false
				}
				err = action.Execute(v)
				if log.WithError(err).
					Debug("executing action", "saga", name, "action", action.Id) {
					return false
				}
				if len(story.Actions) <= actI+1 {
					return true
				}
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
	out = &saga[BT, T]{
		story: story,
		close: cancel,
	}
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

func (s *saga[BT, T]) ExecuteFirst() (err error) {
	err = s.story.Actions[0].task.Create(
		fmt.Sprintf(
			"%s_%s_%s",
			s.story.Name,
			s.story.Actions[0].Id,
			uuid.Must(uuid.NewV7()).String(),
		),
		time.Now(),
		tasks.NoInterval,
		nil,
	)
	/*
		b := s.story.Arcs[0]
		a := b.Actions[0]
		a.Handler = e
		err = b.actions.Create("first_saga_task", time.Now(), tasks.NoInterval, &a)
	*/
	if err != nil {
		return
	}
	return
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
