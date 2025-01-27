package tasks

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/consensus"
	"github.com/iidesho/gober/discovery/local"
	"github.com/iidesho/gober/stream/event/store/inmemory"
)

var (
	ts              Tasks[dd, *dd]
	ts2             Tasks[dd, *dd]
	ts3             Tasks[dd, *dd]
	ctxGlobal       context.Context
	ctxGlobalCancel context.CancelFunc
	testCryptKey    = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
	td              dd
	wg              sync.WaitGroup
	count           int
)

var STREAM_NAME = "TestServiceStoreAndStream_" + uuid.Must(uuid.NewV7()).String()

type dd struct {
	Name string `json:"name"`
	Id   int32  `json:"id"`
}

func (s dd) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteInt32(w, s.Id)
	if err != nil {
		return
	}
	return bcts.WriteTinyString(w, s.Name)
}

func (s *dd) ReadBytes(r io.Reader) (err error) {
	err = bcts.ReadInt32(r, &s.Id)
	if err != nil {
		return
	}
	err = bcts.ReadTinyString(r, &s.Name)
	if err != nil {
		return
	}
	return nil
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

var ct *testing.T

var intervalChan = make(chan struct{})

func executeFunc(d *dd, ctx context.Context) bool {
	ct.Log("Executed", "data", d, "count", count)
	if count > 16 {
		ctxGlobalCancel()
		ct.Error("catchup ran too many times")
		ct.FailNow()
		return true
	}
	if d.Name == "test" && count != 0 {
		ctxGlobalCancel()
		ct.Error("task ran more than once")
		ct.FailNow()
		return false
	}
	count++
	if count == 2 {
		return false
	}
	if count%2 == 0 {
		// return false
	}
	if count == 6 {
		close(intervalChan)
	}
	// time.Sleep(10 * time.Second)

	select {
	case <-time.After(5 * time.Second):
		defer wg.Done()
	case <-ctx.Done():
		return false
	}
	return true
}

func TestInit(t *testing.T) {
	ct = t
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	store, err := inmemory.Init(STREAM_NAME, ctxGlobal)
	// store, err := eventstore.Init()
	if err != nil {
		t.Error(err)
		return
	}
	token := "someTestToken"
	p, err := consensus.Init(3138, token, local.New())
	if err != nil {
		t.Fatal(err)
	}
	edt, err := Init[dd, *dd](
		store,
		p.AddTopic,
		"testdata_schedule1",
		"1.0.0",
		cryptKeyProvider,
		executeFunc,
		time.Second*15,
		false,
		5,
		ctxGlobal,
	)
	if err != nil {
		t.Error(err)
		return
	}
	ts2, err = Init[dd, *dd](
		store,
		p.AddTopic,
		"testdata_schedule2",
		"1.0.0",
		cryptKeyProvider,
		executeFunc,
		time.Second*15,
		true,
		5,
		ctxGlobal,
	)
	if err != nil {
		t.Error(err)
		return
	}
	ts3, err = Init[dd, *dd](
		store,
		p.AddTopic,
		"testdata_schedule3",
		"1.0.0",
		cryptKeyProvider,
		executeFunc,
		time.Second*15,
		false,
		5,
		ctxGlobal,
	)
	if err != nil {
		t.Error(err)
		return
	}
	ts = edt
	go p.Run()
	time.Sleep(time.Microsecond)
	t.Log("init done")
}

func TestCreate(t *testing.T) {
	t.Log("creating single event")
	ct = t
	data := dd{
		Id:   1,
		Name: "test",
	}
	wg.Add(1)
	err := ts.Create("test_task_1", time.Now(), NoInterval, &data)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("created single event")
}

func TestFinish(t *testing.T) {
	t.Log("finishing single event")
	ct = t
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-time.After(30 * time.Second):
		t.Error("timeout on task finish")
	case <-c:
	}
	t.Log("finishied single event")
}

func TestCreateIntervalWithCatchup(t *testing.T) {
	t.Log("creating first task in reacurring task chain")
	ct = t
	data := dd{
		Id:   1,
		Name: "test_interval",
	}
	wg.Add(5)
	err := ts.Create("test_task_recurring", time.Now().Add(-time.Second*100), 10*time.Second, &data)
	if err != nil {
		t.Error(err)
		return
	}
	t.Log("created first task in reacurring task chain")
}

func TestFinishInterval(t *testing.T) {
	t.Log("staring to wait for reacurring tast to be executed 15 times")
	ct = t
	select {
	case <-time.After(60 * time.Second):
		t.Error("timeout on task finish")
	case <-intervalChan:
		wg.Wait()
	}
	t.Log("done waiting for reacurring tast to be executed 15 times")
}

func TestTairdown(t *testing.T) {
	ct = t
	ctxGlobalCancel()
}

var benchRun = 0

func BenchmarkTasks_Create_Select_Finish(b *testing.B) {
	benchRun++
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	store, err := inmemory.Init(fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N), ctx)
	if err != nil {
		b.Error(err)
		return
	}
	token := "someTestToken"
	p, err := consensus.Init(uint16(3143+benchRun), token, local.New())
	if err != nil {
		b.Fatal(err)
	}

	edt, err := Init[dd, *dd](
		store,
		p.AddTopic,
		"testdata",
		"1.0.0",
		cryptKeyProvider,
		func(d *dd, _ context.Context) bool { /*t.Log("benchmark task ran", "data", d);*/ return true },
		time.Second,
		false,
		10,
		ctx,
	) // FIXME: There seems to be an issue with reusing streams
	if err != nil {
		b.Error(err)
		return
	}
	go p.Run()
	data := dd{
		Id:   1,
		Name: "test",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = edt.Create(fmt.Sprintf("bench_task_%d", i), time.Now(), NoInterval, &data)
		if err != nil {
			b.Error(err)
			return
		}
	}
}
