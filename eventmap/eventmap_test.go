package eventmap

import (
	"context"
	"fmt"
	"github.com/cantara/gober/store/eventstore"
	"testing"

	"github.com/google/uuid"
)

var ed EventMap[dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.New().String()

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestInit(t *testing.T) {
	store, err := eventstore.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	edt, err := Init[dd](store, "setandwait", "1.0.0", STREAM_NAME, cryptKeyProvider, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	ed = edt
	return
}

func TestStore(t *testing.T) {
	data := dd{
		Id:   1,
		Name: "test",
	}
	err := ed.Set("1_test", data)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestGet(t *testing.T) {
	data, err := ed.Get("1_test")
	fmt.Println(data)
	if err != nil {
		t.Error(err)
		return
	}
	if data.Id != 1 {
		t.Error(fmt.Errorf("missmatch data id"))
		return
	}
	if data.Name != "test" {
		t.Error(fmt.Errorf("missmatch data name"))
		return
	}
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}
