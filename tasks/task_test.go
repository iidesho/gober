package tasks

import (
	"context"
	"errors"
	"fmt"
	"github.com/cantara/gober/store/inmemory"
	"testing"

	"github.com/google/uuid"
)

var ts Tasks[dd, md]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
var task TaskData[dd]

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.New().String()

type md struct {
	Extra string `json:"extra"`
}

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestInit(t *testing.T) {
	store, err := inmemory.Init()
	if err != nil {
		t.Error(err)
		return
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	edt, err := Init[dd, md](store, "1.0.0", STREAM_NAME, cryptKeyProvider, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}
	ts = edt
	return
}

func TestCreate(t *testing.T) {
	data := dd{
		Id:   1,
		Name: "test",
	}
	meta := md{
		Extra: "extra metadata test",
	}
	err := ts.Create(data, meta)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestSelect(t *testing.T) {
	data, meta, err := ts.Select()
	fmt.Println(data)
	fmt.Println(meta)
	if err != nil {
		t.Error(err)
		return
	}
	if meta.Extra != "extra metadata test" {
		t.Error(fmt.Errorf("missmatch event metadata extra %v", meta))
		return
	}
	task = data
}

func TestFinish(t *testing.T) {
	err := ts.Finish(task.Id)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestSelectAfterFinish(t *testing.T) {
	_, _, err := ts.Select()
	if err == nil {
		t.Error("no error when there shouldn't be anything to select")
		return
	}
	if !errors.Is(err, NothingToSelectError) {
		t.Errorf("error was not NothingToSelectError: %v", err)
		return
	}
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
	ts.Close()
}
