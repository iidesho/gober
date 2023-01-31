package tasks

import (
	"context"
	"fmt"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/store/eventstore"
	"github.com/cantara/gober/store/inmemory"
	"github.com/cantara/gober/stream"
	"github.com/gofrs/uuid"
	"sync"
	"testing"
	"time"
)

var ts Tasks[dd]
var ctxGlobal context.Context
var ctxGlobalCancel context.CancelFunc
var testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
var td dd
var wg sync.WaitGroup

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

type dd struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestInit(t *testing.T) {
	//store, err := inmemory.Init()
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
	edt, err := Init[dd](s, "testdata_schedule", "1.0.0", cryptKeyProvider, func(d dd) bool { log.Println("Executed after time ", d); defer wg.Done(); return true }, ctxGlobal)
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
	wg.Add(1)
	err := ts.Create(time.Now(), NoInterval, data)
	if err != nil {
		t.Error(err)
		return
	}
	return
}

/*
func TestSelect(t *testing.T) {
	data, metad, err := ts.Select()
	fmt.Println(data)
	fmt.Println(metad)
	if err != nil {
		t.Error(err)
		return
	}
	td = data
}
*/

func TestFinish(t *testing.T) {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-time.After(5000 * time.Second):
		t.Error("timeout on task finish")
	case <-c:
	}
}

/*
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
*/

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}

func BenchmarkTasks_Create_Select_Finish(b *testing.B) {
	log.SetLevel(log.ERROR)
	store, err := inmemory.Init()
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

	edt, err := Init[dd](s, "testdata", "1.0.0", cryptKeyProvider, func(d dd) bool { log.Println(d); return true }, ctxGlobal) //FIXME: There seems to be an issue with reusing streams
	if err != nil {
		b.Error(err)
		return
	}
	data := dd{
		Id:   1,
		Name: "test",
	}
	for i := 0; i < b.N; i++ {
		err = edt.Create(time.Now(), NoInterval, data)
		if err != nil {
			b.Error(err)
			return
		}
	}
}
