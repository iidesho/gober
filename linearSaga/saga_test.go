package saga_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/itr"
	saga "github.com/iidesho/gober/linearSaga"
	"github.com/iidesho/gober/stream/event/store/mariadb"
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
	os.Remove("./streams")
	store, err := mariadb.Init("t", context.Background())
	// store, err := sharding.NewShardedStream("test",
	// 	func(name string, ctx context.Context) (stream.Stream, error) {
	// 		return inmemory.Init(name, ctx)
	// 	},
	// 	sharding.StaticRouter{}.Route,
	// 	context.Background())
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

func TestReadErrsIDNA(t *testing.T) {
	_, _, err := s.ReadErrors(uuid.Must(uuid.NewV4()), context.Background())
	if !errors.Is(err, saga.ErrExecutionNotFound) {
		t.Fatal("expected error for non existing ID in errors")
	}
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

type actBM struct{}

func (a actBM) Execute(s *bcts.TinyString, ctx context.Context) error {
	// log.Info("executing actBM", "s", *s)
	return nil
}

func (a actBM) Reduce(s *bcts.TinyString, ctx context.Context) error {
	// log.Info("recucing actBM", "s", *s)
	return nil
}

var stryBM = saga.Story[bcts.TinyString, *bcts.TinyString]{
	Name: "testBM",
	Actions: []saga.Action[bcts.TinyString, *bcts.TinyString]{
		{
			ID:      "action_1",
			Handler: &actBM{},
		},
		{
			ID:      "action_2",
			Handler: &actBM{},
		},
		{
			ID:      "action_3_pre_done",
			Handler: &actBM{},
		},
		{
			ID:      "action_4",
			Handler: &actBM{},
		},
	},
}

var (
	edt    saga.Saga[bcts.TinyString, *bcts.TinyString]
	i      = 2
	allIDS = make([]uuid.UUID, 0)
)

func BenchmarkSagaNew(b *testing.B) {
	os.RemoveAll("./streams")
	// ... saga Init ...
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // This will cancel the executor's context when the benchmark finishes
	startGoroutines := runtime.NumGoroutine()
	b.Logf("Starting with %d goroutines", startGoroutines)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Info("runtime stats",
					"goroutines", runtime.NumGoroutine(),
				)
			}
		}
	}()

	// store, err := sharding.NewShardedStream(fmt.Sprintf("bench-%d", time.Now().Unix()),
	// 	func(name string, ctx context.Context) (stream.Stream, error) {
	// 		// return ondisk.Init(name, ctx)
	// 		return inmemory.Init(name, ctx)
	// 	},
	// 	sharding.StaticRouter{}.Route,
	// 	ctx)
	store, err := mariadb.Init(fmt.Sprintf("bench-%d", time.Now().Unix()), ctx)
	if err != nil {
		cancel()
		b.Error(err)
		return
	}
	serv, err := webserver.Init(uint16(3133+i), true)
	i += 4
	if err != nil {
		cancel()
		b.Fatal(err)
	}
	edt, err = saga.Init(
		store,
		serv,
		"1.0.0",
		stryBM,
		cryptKeyProvider,
		runtime.NumCPU()*100,
		ctx,
	)
	if err != nil {
		cancel()
		b.Error(err)
		return
	}
	defer edt.Close()
	go serv.Run()
	defer serv.Shutdown()
	defer cancel()

	var wg sync.WaitGroup
	sagaErrs := make(chan error, b.N) // Buffer for errors from all sagas

	bv := bcts.TinyString(fmt.Sprintf("BENCHMARK_%d", b.N))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sagaCtx, sagaCancel := context.WithCancel(
			context.Background(),
		) // Context for this specific saga
		sagaID, err := edt.ExecuteFirst(bv, sagaCtx)
		if err != nil {
			sagaCancel()
			b.Errorf("Failed to start saga: %v", err)
			continue
		}

		errCh, getStateFn, err := edt.ReadErrors(sagaID, sagaCtx)
		if err != nil {
			sagaCancel()
			b.Errorf("Failed to read errors for saga %s: %v", sagaID, err)
			continue
		}

		wg.Add(1)
		go func(id uuid.UUID, cancelCtx context.CancelFunc, errs <-chan error, stateFn func() saga.State) {
			defer wg.Done()
			defer cancelCtx() // Crucial: Cancel the saga's context when this goroutine exits

			for {
				select {
				case err, ok := <-errs:
					if !ok { // Error channel closed, saga reader goroutine terminated
						return
					}
					sagaErrs <- fmt.Errorf("Saga %s error: %v", id, err)
				case <-time.After(10 * time.Second): // Timeout for saga completion (adjust as needed)
					currentState := stateFn()
					if currentState != saga.StatePending && currentState != saga.StateWorking {
						// Saga should have completed/failed by now
						sagaErrs <- fmt.Errorf("Saga %s timed out waiting for completion, final state: %s", id, currentState)
						return
					}
					// Still pending/working, continue waiting
				}
				// Check saga state to know when it's truly done
				currentState := stateFn()
				if currentState == saga.StateSuccess || currentState == saga.StateFailed ||
					currentState == saga.StatePaniced {
					return // Saga completed, exit goroutine
				}
			}
		}(
			sagaID,
			sagaCancel,
			errCh,
			getStateFn,
		)
	}

	// Wait for all sagas to complete or timeout
	wg.Wait()
	b.StopTimer()

	// Check for any collected errors
	close(sagaErrs)
	for err := range sagaErrs {
		b.Error(err)
	}
}

func BenchmarkSaga(b *testing.B) {
	os.RemoveAll("./streams")
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	startGoroutines := runtime.NumGoroutine()
	b.Logf("Starting with %d goroutines", startGoroutines)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				log.Info("runtime stats",
					"goroutines", runtime.NumGoroutine(),
				)
			}
		}
	}()
	// if edt == nil {
	// store, err := ondisk.Init("b", context.Background())
	store, err := mariadb.Init("b", context.Background())
	// if err != nil {
	// 	b.Error(err)
	// 	return
	// }
	// store, err := sharding.NewShardedStream(fmt.Sprintf("bench-%d", time.Now().Unix()),
	// 	func(name string, ctx context.Context) (stream.Stream, error) {
	// 		return ondisk.Init(name, ctx)
	// 		// return inmemory.Init(name, ctx)
	// 	},
	// 	sharding.StaticRouter{}.Route,
	// 	ctx)
	if err != nil {
		cancel()
		b.Error(err)
		return
	}
	serv, err := webserver.Init(uint16(3133+i), true)
	i += 4
	if err != nil {
		cancel()
		b.Fatal(err)
	}
	edt, err = saga.Init(
		store,
		serv,
		"1.0.0",
		stryBM,
		cryptKeyProvider,
		runtime.NumCPU()*100,
		ctx,
	)
	if err != nil {
		cancel()
		b.Error(err)
		return
	}
	defer edt.Close()
	go serv.Run()
	defer serv.Shutdown()
	defer cancel()
	// }
	bv := bcts.TinyString(fmt.Sprintf("BENCHMARK_%d", b.N))
	ids := make([]uuid.UUID, b.N)
	for i := 0; i < b.N; i++ {
		id, err := edt.ExecuteFirst(bv, ctx)
		if err != nil {
			b.Error(err)
			return
		}
		if itr.NewIterator(allIDS).Contains(func(v uuid.UUID) bool {
			return v.String() == id.String()
		}) {
			b.Error("generated equal id")
			return
		}
		allIDS = append(allIDS, id)
		ids[i] = id
		// fmt.Println("executed first", "time", time.Now(), "i", i, "id", ids[i].String())
	}
	b.Logf("started %d saga executions", b.N)
	for i := 0; i < b.N; i++ {
		// fmt.Println("reading errors", "time", time.Now(), "i", i, "id", ids[i].String())
		errs, status, err := edt.ReadErrors(ids[i], ctx)
		if err != nil {
			b.Error(err)
			return
		}
		done := false
		for !done {
			// fmt.Println("pre:", time.Now())
			select {
			case <-time.NewTimer(time.Hour * 3).C:
				b.Error("timed out")
				return
			case _, ok := <-errs:
				if !ok {
					done = true
				}
				runtime.Gosched()
			}
		}
		// cancel()

		state := status()
		if state != saga.StateSuccess {
			b.Error("saga failed", "status", state)
			return
		}
	}
	b.Logf("completed %d saga executions", b.N)
}
