package saga

import (
	"context"
	"errors"
	"fmt"
	event "github.com/cantara/gober"
	"github.com/cantara/gober/tasks"
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

type Action struct {
	Id       string   `json:"id"`
	Requires []string `json:"requires"`
	Body     interface {
		Execute() error
	} `json:"body"`
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

func Init[MT any](pers event.Persistence, dataTypeVersion, name string, story Story, p event.CryptoKeyProvider, ctx context.Context) (out *saga[MT], err error) {
	ctxTask, cancel := context.WithCancel(ctx)
	actions, err := tasks.Init[Action, MT](pers, dataTypeVersion, name, p, ctxTask)
	if err != nil {
		cancel()
		return
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
