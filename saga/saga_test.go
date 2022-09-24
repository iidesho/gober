package saga

import (
	"context"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/store/eventstore"
	"github.com/cantara/gober/store/inmemory"
	"github.com/gofrs/uuid"
	"testing"
)

var s Saga
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="

var STREAM_NAME = "TestSaga_" + uuid.Must(uuid.NewV7()).String()

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

type act1 struct {
	Inner string `json:"inner"`
}

func (a act1) Execute() error {
	fmt.Println(a.Inner)
	return nil
}

type act2 struct {
	Pre  string `json:"pre"`
	Post string `json:"post"`
}

func (a act2) Execute() error {
	fmt.Println(a.Pre, "woop", a.Post)
	return nil
}

var stry = Story{
	Name: "test",
	Arcs: []Arc{
		{
			Actions: []Action{
				{
					Id: "action_1",
					Body: act1{
						Inner: "test",
					},
				},
			},
		},
		{
			Actions: []Action{
				{
					Id: "action_3",
					Body: act1{
						Inner: "test",
					},
				},
				{
					Id: "action_2",
					Body: act2{
						Pre:  "bef",
						Post: "aft",
					},
				},
			},
		},
	},
}

func TestInit(t *testing.T) {
	log.SetLevel(log.INFO)
	store, err := inmemory.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	edt, err := Init(store, "1.0.0", STREAM_NAME, stry, cryptKeyProvider, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	s = edt
	return
}

func TestExecuteFirst(t *testing.T) {
	err := s.ExecuteFirst(stry.Arcs[0].Actions[0].Body)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
	s.Close()
}

func BenchmarkSaga(b *testing.B) {
	log.SetLevel(log.ERROR)
	store, err := eventstore.Init()
	if err != nil {
		b.Error(err)
		return
	}
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	edt, err := Init(store, "1.0.0", fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), stry, cryptKeyProvider, ctx)
	if err != nil {
		b.Error(err)
		return
	}
	defer edt.Close()
	for i := 0; i < b.N; i++ {
		err = edt.ExecuteFirst(stry.Arcs[0].Actions[0].Body)
		if err != nil {
			b.Error(err)
			return
		}
	}
}
