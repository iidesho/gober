package saga_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	saga "github.com/iidesho/gober/linearSaga"
	"github.com/iidesho/gober/stream/event/store/inmemory"
	"github.com/iidesho/gober/webserver"
)

var (
	s               saga.Saga[bcts.TinyString, *bcts.TinyString]
	ctxGlobal       context.Context
	ctxGlobalCancel context.CancelFunc
	testCryptKey    = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
	log             = sbragi.WithLocalScope(sbragi.LevelDebug)
)

var (
	STREAM_NAME = "TestSaga_" + uuid.Must(uuid.NewV7()).String()
	wg          = &sync.WaitGroup{}
)

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

func (a *act1) Execute(s *bcts.TinyString) (saga.State, error) {
	log.Info("executing act1")
	defer wg.Done()
	log.Info(a.Inner, "s", s)
	*s = bcts.TinyString(fmt.Sprint(*s, "-", a.Inner))
	a.State = saga.StateSuccess
	return a.State, nil
}

func (a act1) Reduce(*bcts.TinyString) (saga.State, error) {
	return saga.StateSuccess, nil
}

/*
func (a act1) Status(s *bcts.TinyString)  {
	sbragi.Info("act1 status", "data", *s, "contains", strings.Contains(string(*s), "init"))
	if !strings.Contains(string(*s), "init") {
		return saga.StateInvalid, nil
	}
	return a.State, nil
}
*/

type act2 struct {
	Pre    string `json:"pre"`
	Post   string `json:"post"`
	Failed bool   `json:"failed"`
	State  saga.State
}

func (a *act2) Execute(*bcts.TinyString) (saga.State, error) {
	log.Info("executing act2", "pre", a.Pre, "post", a.Post)
	if a.State == saga.StateSuccess {
		return a.State, nil
	}
	if !a.Failed {
		a.Failed = true
		return saga.StateFailed, saga.RetryableError("action_2", nil)
	}
	defer wg.Done()
	log.Info(a.Pre, "woop", a.Post)
	a.State = saga.StateSuccess
	return a.State, nil
}

func (a act2) Reduce(*bcts.TinyString) (saga.State, error) {
	return saga.StateSuccess, nil
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

var a2D = act2{
	Pre:   "pre",
	Post:  "done",
	State: saga.StateSuccess,
}

var a3 = act1{
	Inner: "test2",
	State: saga.StatePending,
}

var stry = saga.Story[bcts.TinyString, *bcts.TinyString]{
	Name: "test",
	Actions: []saga.Action[bcts.TinyString, *bcts.TinyString]{
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
			Id:      "action_2_pre_done",
			Handler: &a2D,
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
	serv, err := webserver.Init(3132, true)
	if err != nil {
		t.Fatal(err)
	}
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	edt, err := saga.Init(
		store,
		serv,
		"1.0.0",
		STREAM_NAME,
		stry,
		cryptKeyProvider,
		runtime.NumCPU(),
		ctxGlobal,
	)
	if err != nil {
		t.Error(err)
		return
	}
	s = edt
	go serv.Run()
	time.Sleep(time.Second)
}

var id uuid.UUID

func TestExecuteFirst(t *testing.T) {
	wg.Add(3)
	var err error
	v := bcts.TinyString("init")
	id, err = s.ExecuteFirst(&v)
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
	serv, err := webserver.Init(3132, true)
	if err != nil {
		b.Fatal(err)
	}
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	edt, err := saga.Init(
		store,
		serv,
		"1.0.0",
		fmt.Sprintf("%s_%s-%d", STREAM_NAME, b.Name(), b.N),
		stry,
		cryptKeyProvider,
		runtime.NumCPU(),
		ctx,
	)
	if err != nil {
		b.Error(err)
		return
	}
	defer edt.Close()
	go serv.Run()
	for i := 0; i < b.N; i++ {
		_, err = edt.ExecuteFirst(nil)
		if err != nil {
			b.Error(err)
			return
		}
	}
}
