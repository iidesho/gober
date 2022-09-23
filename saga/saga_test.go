package saga

import (
	"context"
	"fmt"
	"github.com/cantara/gober/store/inmemory"
	"github.com/gofrs/uuid"
	"go/types"
	"testing"
)

var s Saga[dd, types.Nil]
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
	inner string
}

func (a act1) Execute() error {
	fmt.Println(a.inner)
	return nil
}

type act2 struct {
	pre  string
	post string
}

func (a act2) Execute() error {
	fmt.Println(a.pre, "woop", a.post)
	return nil
}

var stry = Story{
	Name: "test",
	Actions: []Action{
		{
			Id: "action_1",
			Body: act1{
				inner: "test",
			},
		},
		{
			Id:       "action_2",
			Requires: []string{"action_1"},
			Body: act2{
				pre:  "bef",
				post: "aft",
			},
		},
	},
}

func TestInit(t *testing.T) {
	store, err := inmemory.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	edt, err := Init[types.Nil](store, "1.0.0", STREAM_NAME, stry, cryptKeyProvider, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	s = edt
	return
}

func TestPrime(t *testing.T) {
	err := s.Prime(stry)
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
