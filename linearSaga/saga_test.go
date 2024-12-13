package saga_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	saga "github.com/iidesho/gober/linearSaga"
	"github.com/iidesho/gober/stream/event/store/inmemory"
)

var s saga.Saga[bcts.Nil, *bcts.Nil]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
var log = sbragi.WithLocalScope(sbragi.LevelDebug)

var STREAM_NAME = "TestSaga_" + uuid.Must(uuid.NewV7()).String()
var wg = &sync.WaitGroup{}

type dd struct {
	Name string `json:"name"`
	Id   int    `json:"id"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

type act1 struct {
	Inner string `json:"inner"`
	State saga.State
}

func (a *act1) Execute(*bcts.Nil) error {
	defer wg.Done()
	log.Info(a.Inner)
	a.State = saga.StateSuccess
	return nil
}
func (a act1) Reduce(*bcts.Nil) error {
	return nil
}
func (a act1) Status(*bcts.Nil) (saga.State, error) {
	return a.State, nil
}

type act2 struct {
	Pre    string `json:"pre"`
	Post   string `json:"post"`
	Failed bool   `json:"failed"`
	State  saga.State
}

func (a *act2) Execute(*bcts.Nil) error {
	if !a.Failed {
		a.Failed = true
		return saga.ErrRetryable
	}
	defer wg.Done()
	log.Info(a.Pre, "woop", a.Post)
	a.State = saga.StateSuccess
	return nil
}
func (a act2) Reduce(*bcts.Nil) error {
	return nil
}
func (a act2) Status(*bcts.Nil) (saga.State, error) {
	return a.State, nil
}

var a1 = act1{
	Inner: "test",
	State: saga.StatePending,
}
var a2 = act2{
	Pre:   "bef",
	Post:  "aft",
	State: saga.StatePending,
}
var a3 = act1{
	Inner: "test2",
	State: saga.StatePending,
}

var stry = saga.Story[bcts.Nil, *bcts.Nil]{
	Name: "test",
	Actions: []saga.Action[bcts.Nil, *bcts.Nil]{
		{
			Id:      "action_1",
			Handler: &a1,
			/*
				Status:  a1.Status,
				Execute: a1.Execute,
				Reduce:  a1.Reduce,
			*/
		},
		{
			Id:      "action_2",
			Handler: &a2,
			/*
				Status:  a2.Status,
				Execute: a2.Execute,
				Reduce:  a2.Reduce,
			*/
		},
		{
			Id:      "action_3",
			Handler: &a3,
			/*
				Status:  a3.Status,
				Execute: a3.Execute,
				Reduce:  a3.Reduce,
			*/
		},
	},
}

func TestInit(t *testing.T) {
	store, err := inmemory.Init("t", context.Background())
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	edt, err := saga.Init(store, "1.0.0", STREAM_NAME, stry, cryptKeyProvider, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	s = edt
}

var id uuid.UUID

func TestExecuteFirst(t *testing.T) {
	wg.Add(3)
	var err error
	id, err = s.ExecuteFirst(nil)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestTairdown(t *testing.T) {
	wg.Wait()
	st, err := s.Status(id)
	if err != nil {
		t.Error(err, id)
		return
	}
	if st != saga.StateSuccess {
		t.Error("expected completed saga to be success", st.String())
		return
	}
	ctxGlobalCancel()
	s.Close()
}

func BenchmarkSaga(b *testing.B) {
	store, err := inmemory.Init("b", context.Background())
	if err != nil {
		b.Error(err)
		return
	}
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	edt, err := saga.Init(
		store,
		"1.0.0",
		fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N),
		stry,
		cryptKeyProvider,
		ctx,
	)
	if err != nil {
		b.Error(err)
		return
	}
	defer edt.Close()
	for i := 0; i < b.N; i++ {
		_, err = edt.ExecuteFirst(nil)
		if err != nil {
			b.Error(err)
			return
		}
	}
}