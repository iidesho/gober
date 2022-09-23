package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	event "github.com/cantara/gober"
	"github.com/cantara/gober/tasks"
	"reflect"
	"time"
)

type Saga[DT, MT any] interface {
	Prime(story Story) error
	Close()
}

type transactionCheck struct {
	transaction  uint64
	completeChan chan struct{}
}

type saga[MT any] struct {
	story   Story
	actions tasks.Tasks[Action, MT]
	close   func()
}

type executor interface {
	Execute() error
}

type Action struct {
	Id       string   `json:"id"`
	Requires []string `json:"requires"`
	Type     string   `json:"type"` //I might want to simplify this.
	Body     executor `json:"body"`
}

type Story struct {
	Name    string
	Actions []Action
}
type taskChanData[MT any] struct {
	task   tasks.TaskData[Action]
	taskMD MT
	err    error
}

var executors []interface{}

func (a *Action) UnmarshalJSON(data []byte) error {

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	var tmpBody map[string]interface{}
	val := reflect.ValueOf(*a)
	for i := 0; i < val.Type().NumField(); i++ {
		switch val.Type().Field(i).Name {
		case "Id":
			a.Id = m[val.Type().Field(i).Tag.Get("json")].(string)
		case "Requires":
			reqI := m[val.Type().Field(i).Tag.Get("json")]
			if reqI == nil {
				continue
			}
			var req []string
			for _, item := range reqI.([]interface{}) {
				req = append(req, item.(string))
			}
			a.Requires = req
		case "Type":
			a.Type = m[val.Type().Field(i).Tag.Get("json")].(string)
		case "Body":
			tmpBody = m[val.Type().Field(i).Tag.Get("json")].(map[string]interface{})
		}
	}
	for _, e := range executors { //I would like a way to not use this global version.
		re := reflect.ValueOf(e)
		if a.Type != re.Type().String() {
			continue
		}
		for i := 0; i < re.Type().NumField(); i++ {
			if v, ok := tmpBody[re.Type().Field(i).Tag.Get("json")]; ok {
				re.Field(i).Set(reflect.ValueOf(v))
			}
		}
		a.Body = re.Interface().(executor)
	}
	return nil
}

func Init[MT any](pers event.Persistence, dataTypeVersion, name string, story Story, p event.CryptoKeyProvider, ctx context.Context) (out *saga[MT], err error) {
	ctxTask, cancel := context.WithCancel(ctx)
	actions, err := tasks.Init[Action, MT](pers, dataTypeVersion, name, p, ctxTask)
	if err != nil {
		cancel()
		return
	}
	for i, a := range story.Actions {
		ra := reflect.ValueOf(a)
		story.Actions[i].Type = ra.Type().String()
		for _, e := range executors { //I would like a way to not use this global version.
			if ra.Type() == reflect.ValueOf(e).Type() {
				continue
			}
			executors = append(executors, a)
		}
	}
	out = &saga[MT]{
		story:   story,
		actions: actions,
		close:   cancel,
	}
	selectChan := make(chan struct{}, 0)
	taskChan := make(chan taskChanData[MT], 0)
	go func() {
		for range selectChan {
			t, m, e := actions.Select()
			if errors.Is(e, tasks.NothingToSelectError) {
				time.Sleep(500 * time.Millisecond)
				continue
			}
			taskChan <- taskChanData[MT]{
				task:   t,
				taskMD: m,
				err:    e,
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
				fmt.Println(selected)
			}
		}
	}()
	return
}

func (s *saga[MT]) Prime(story Story) (err error) {
	var t MT
	for _, a := range story.Actions {
		ra := reflect.ValueOf(a)
		a.Type = ra.Type().String()
		err = s.actions.Create(a, t)
		if err != nil {
			return
		}
	}
	return
}

func (s *saga[MT]) Close() {
	s.close()
}
