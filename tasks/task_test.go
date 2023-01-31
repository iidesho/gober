package tasks

import (
	"context"
	"errors"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/store/eventstore"
	"github.com/cantara/gober/stream"
	"github.com/gofrs/uuid"
	"testing"
)

var ts Tasks[dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
var task TaskData[dd]

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

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
	s, err := stream.Init(store, STREAM_NAME, ctxGlobal)
	if err != nil {
		return
	}
	edt, err := Init[dd](s, "testdata", "1.0.0", cryptKeyProvider, ctxGlobal)
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
	err := ts.Create(uuid.Must(uuid.NewV7()), data)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

func TestSelect(t *testing.T) {
	data, err := ts.Select()
	fmt.Println(data)
	if err != nil {
		t.Error(err)
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
	_, err := ts.Select()
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
}

func BenchmarkTasks_Create_Select_Finish(b *testing.B) {
	log.SetLevel(log.ERROR)
	store, err := eventstore.Init()
	if err != nil {
		b.Error(err)
		return
	}
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	s, err := stream.Init(store, fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		return
	}
	edt, err := Init[dd](s, "testdata", "1.0.0", cryptKeyProvider, ctx) //FIXME: There seems to be an issue with reusing streams
	if err != nil {
		b.Error(err)
		return
	}
	data := dd{
		Id:   1,
		Name: "test",
	}
	for i := 0; i < b.N; i++ {
		err = edt.Create(uuid.Must(uuid.NewV7()), data)
		if err != nil {
			b.Error(err)
			return
		}
	}
	ids := make([]uuid.UUID, b.N)
	var td TaskData[dd]
	for i := 0; i < b.N; i++ {
		td, err = edt.Select()
		if err != nil {
			b.Error(err)
			return
		}
		ids[i] = td.Id
	}
	for i := 0; i < b.N; i++ {
		err = edt.Finish(ids[i])
		if err != nil {
			b.Error(err)
			return
		}
	}
}
