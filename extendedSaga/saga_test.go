package saga_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	saga "github.com/iidesho/gober/extendedSaga"
	"github.com/iidesho/gober/stream/event/store/inmemory"
)

var s saga.Saga
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
}

func (a act1) Execute() error {
	defer wg.Done()
	log.Info(a.Inner)
	return nil
}
func (a act1) Reduce() error {
	return nil
}
func (a act1) Status() (saga.State, error) {
	return saga.StatePending, nil
}

type act2 struct {
	Pre  string `json:"pre"`
	Post string `json:"post"`
}

func (a act2) Execute() error {
	defer wg.Done()
	log.Info(a.Pre, "woop", a.Post)
	return nil
}
func (a act2) Reduce() error {
	return nil
}
func (a act2) Status() (saga.State, error) {
	return saga.StatePending, nil
}

var stry = saga.Story{
	Name: "test",
	Arcs: []saga.Arc{
		{
			Actions: []saga.Action{
				{
					Id: "action_1",
					Handler: act1{
						Inner: "test",
					},
				},
			},
		},
		{
			Actions: []saga.Action{
				{
					Id: "action_3",
					Handler: act1{
						Inner: "test",
					},
				},
				{
					Id: "action_2",
					Handler: act2{
						Pre:  "bef",
						Post: "aft",
					},
				},
			},
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

func TestExecuteFirst(t *testing.T) {
	wg.Add(3)
	err := s.ExecuteFirst()
	if err != nil {
		t.Error(err)
		return
	}
}

func TestTairdown(t *testing.T) {
	wg.Wait()
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
		err = edt.ExecuteFirst()
		if err != nil {
			b.Error(err)
			return
		}
	}
}
