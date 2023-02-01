package saga

import (
	"context"
	"encoding/json"
	"errors"
	event "github.com/cantara/gober"
	"github.com/cantara/gober/tasks"
	"reflect"
)

type Saga interface {
	ExecuteFirst(e executor) error
	Close()
}

type transactionCheck struct {
	transaction  uint64
	completeChan chan struct{}
}

type saga struct {
	story Story
	close func()
}

type executor interface {
	Execute() error
}

type Action struct {
	Id string `json:"id"`
	//Requires []string `json:"requires"`
	Type string   `json:"type"` //I might want to simplify this.
	Body executor `json:"body"`
}

type Arc struct {
	Actions []Action
	actions tasks.Tasks[Action, any]
}

type Story struct {
	Name string
	Arcs []Arc
}
type taskChanData struct {
	task tasks.TaskData[Action]
	err  error
}

var executors []struct {
	e interface{}
	v reflect.Value
	t reflect.Type
}

func (a *Action) UnmarshalJSON(data []byte) error {
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	var tmpBody map[string]interface{}
	val := reflect.ValueOf(*a)
	typ := val.Type()
	//typ := reflect.TypeOf(*a) //Use this instead of val.Type?
	for i := 0; i < typ.NumField(); i++ {
		switch typ.Field(i).Name {
		case "Id":
			a.Id = m[typ.Field(i).Tag.Get("json")].(string)
		case "Type":
			a.Type = m[typ.Field(i).Tag.Get("json")].(string)
		case "Body":
			tmpBody = m[typ.Field(i).Tag.Get("json")].(map[string]interface{})
		}
	}
	for _, e := range executors { //I would like a way to not use this global version.
		if a.Type != e.t.String() {
			continue
		}
		for i := 0; i < e.t.NumField(); i++ {
			if v, ok := tmpBody[e.t.Field(i).Tag.Get("json")]; ok {
				e.v.Field(i).Set(reflect.ValueOf(v))
			}
		}
		a.Body = e.v.Interface().(Action).Body
	}
	return nil
}

func Init(pers event.Persistence, dataTypeVersion, name string, story Story,
	p event.CryptoKeyProvider, ctx context.Context) (out *saga, err error) {
	if len(story.Arcs) <= 1 {
		err = NotEnoughArcsError
		return
	}
	if len(story.Arcs[0].Actions) != 1 {
		err = BaseArcActionMismatchError
		return
	}
	ctxTask, cancel := context.WithCancel(ctx)
	for li, l := range story.Arcs {
		var actions tasks.Tasks[Action, any]
		actions, err = tasks.Init[Action, any](pers, dataTypeVersion, name, p, ctxTask)
		if err != nil {
			cancel()
			return
		}
		story.Arcs[li].actions = actions
		for ai, a := range l.Actions {
			ra := reflect.ValueOf(a)
			story.Arcs[li].Actions[ai].Type = ra.Type().String()
			executorExists := false
			for _, e := range executors { //I would like a way to not use this global version.
				if ra.Type() == reflect.ValueOf(e).Type() {
					executorExists = true
					break
				}
			}
			if !executorExists {
				executors = append(executors, struct { //TODO: Clean this up.
					e interface{}
					v reflect.Value
					t reflect.Type
				}{e: a, v: ra, t: ra.Type()})
			}
		}
	}
	out = &saga{
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

func (s *saga) ExecuteFirst(e executor) (err error) {
	b := s.story.Arcs[0]
	a := b.Actions[0]
	a.Body = e
	err = b.actions.Create(a, nil)
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

func (s *saga) Close() {
	s.close()
}

var NotEnoughArcsError = errors.New("a story need more than one arc")
var BaseArcActionMismatchError = errors.New("base arc can only and needs one action")
