package saga_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"
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

var (
	expectedSuccess = []string{
		"1 fail",
		"1 reduce",
		"1 success",
		"2 fail",
		"2 reduce",
		"2 success",
		"2 pre done",
		"1 success",
	}
	expectedFail = []string{
		"1 fail",
	}
	has = []string{}
)

func (a *act1) Execute(s *bcts.TinyString, ctx context.Context) error {
	log.Info("executing act1", "s", *s)
	if *s == "FAIL" {
		defer wg.Done()
		has = append(has, "1 fail")
		return errors.New("failing act1")
	}
	if len(string(*s)) > 4 {
		if !strings.HasSuffix(string(*s), a.Inner) {
			*s = bcts.TinyString(fmt.Sprint(*s, "-", a.Inner))
		}
		defer wg.Done()
		has = append(has, "1 success")
		a.State = saga.StateSuccess
		return nil
	}
	log.Info(a.Inner, "s", s)
	*s = bcts.TinyString(fmt.Sprint(*s, "-", a.Inner))
	has = append(has, "1 fail")
	return saga.RetryableError("action_1", nil)
}

func (a act1) Reduce(s *bcts.TinyString, ctx context.Context) error {
	log.Info("recucing act 1", "s", *s)
	has = append(has, "1 reduce")
	return nil
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

func (a *act2) Execute(s *bcts.TinyString, ctx context.Context) error {
	log.Info("executing act2", "s", *s, "pre", a.Pre, "post", a.Post)
	if a.State == saga.StateSuccess {
		has = append(has, "2 pre done")
		return nil
	}
	if !a.Failed {
		a.Failed = true
		has = append(has, "2 fail")
		return saga.RetryableError("action_2", nil)
	}
	defer wg.Done()
	log.Info(a.Pre, "woop", a.Post)
	a.State = saga.StateSuccess
	has = append(has, "2 success")
	return nil
}

func (a act2) Reduce(s *bcts.TinyString, ctx context.Context) error {
	log.Info("recucing act 2", "s", *s)
	has = append(has, "2 reduce")
	return nil
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
			ID:      "action_1",
			Handler: &a1,
			/*
				Status:  a1.Status,
				Execute: a1.Execute,
				Reduce:  a1.Reduce,
			*/
		},
		{
			ID:      "action_2",
			Handler: &a2,
			/*
				Status:  a2.Status,
				Execute: a2.Execute,
				Reduce:  a2.Reduce,
			*/
		},
		{
			ID:      "action_3_pre_done",
			Handler: &a2D,
			/*
				Status:  a2.Status,
				Execute: a2.Execute,
				Reduce:  a2.Reduce,
			*/
		},
		{
			ID:      "action_4",
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
	id, err = s.ExecuteFirst(v, context.Background())
	if err != nil {
		t.Error(err)
		return
	}
}

func TestTairdown(t *testing.T) {
	errs, status, err := s.ReadErrors(id, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	time.Sleep(time.Second)
forLoop:
	for {
		select {
		case err, ok := <-errs:
			if !ok {
				break forLoop
			}
			log.WithError(err).Notice("success saga error")
		default:
			t.Error("expected to have error in err chan", status().String())
			return
		}
	}
	if status() != saga.StateSuccess {
		t.Error("expected completed saga to be success", status().String())
		return
	}
	if !slices.Equal(expectedSuccess, has) {
		t.Error("expected saga order missmatch", expectedSuccess, has)
		return
	}
}

func TestExecuteFirstFail(t *testing.T) {
	has = []string{}
	wg.Add(1)
	var err error
	v := bcts.TinyString("FAIL")
	id, err = s.ExecuteFirst(v, context.Background())
	if err != nil {
		t.Error(err)
		return
	}
}

func TestTairdownFail(t *testing.T) {
	errs, status, err := s.ReadErrors(id, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	time.Sleep(time.Second * 10)
forLoop:
	for {
		select {
		case err, ok := <-errs:
			if !ok {
				break forLoop
			}
			log.WithError(err).Notice("fail saga error")
		default:
			t.Error("expected to have error in err chan", status().String())
			return
		}
	}
	if status() != saga.StateFailed {
		t.Error("expected completed saga to be failed", status().String())
		return
	}
	if !slices.Equal(expectedFail, has) {
		t.Error("expected saga order missmatch", expectedFail, has)
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
	bv := bcts.TinyString(strconv.Itoa(b.N))
	for i := 0; i < b.N; i++ {
		_, err := edt.ExecuteFirst(bv, context.Background())
		if err != nil {
			b.Error(err)
			return
		}
	}
}
