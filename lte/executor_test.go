package lte_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	stdSync "sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/consensus/contenious"
	"github.com/iidesho/gober/discovery"
	"github.com/iidesho/gober/lte"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event/store/inmemory"
	"github.com/iidesho/gober/sync"
	"github.com/iidesho/gober/webserver"
)

type local struct {
	nodes []string
}

func New(nodes []string) discovery.Discoverer {
	return &local{nodes: nodes}
}

func (l *local) Servers() []string {
	return l.nodes
}

func (l *local) Self() string {
	return l.nodes[0]
}

var e lte.Executor[bcts.TinyString, *bcts.TinyString]

var (
	id1 = uuid.Must(uuid.NewV7())
	id2 = uuid.Must(uuid.NewV7())
	id3 = uuid.Must(uuid.NewV7())
	fc  = atomic.Int32{}
	wg  = stdSync.WaitGroup{}
	str = ""
)

func TestInit(t *testing.T) {
	ctx := context.TODO()
	dl, _ := sbragi.NewLogger(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource:   true,
		Level:       sbragi.LevelTrace,
		ReplaceAttr: sbragi.ReplaceAttr,
	}))
	dl.SetDefault()
	sbragi.Info("Initializing consensus")
	token := sync.NewObj[string]()
	token.Set("someTestToken")
	topic := "test"
	serv, err := webserver.Init(3132, true)
	if err != nil {
		t.Fatal(err)
	}
	p, err := contenious.New(
		serv,
		token,
		&local{nodes: []string{"localhost:3132"}},
		topic,
		ctx,
	)
	if err != nil {
		t.Fatal(err)
	}
	s, err := inmemory.Init("lte-test", ctx)
	if err != nil {
		t.Fatal(err)
	}
	wg.Add(3)
	e, err = lte.Init(
		s,
		p,
		"test-string",
		"v0.0.1",
		stream.StaticProvider("aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="),
		func(t *bcts.TinyString, ctx context.Context) error {
			time.Sleep(time.Second)
			if *t == "t2" {
				time.Sleep(time.Millisecond * 500)
			}
			fmt.Println(*t)
			if *t == "t3" && fc.CompareAndSwap(0, 1) {
				fmt.Println("failing t3")
				// fc.Add(1)
				return fmt.Errorf("fail t3")
			}
			str = str + string(*t)
			wg.Done()
			return nil
		},
		5,
		ctx,
	)

	go serv.Run()
	time.Sleep(time.Second)
}

func TestLTEAlone(t *testing.T) {
	t1 := "t1"
	t1bc := bcts.TinyString(t1)
	t2 := "t2"
	t2bc := bcts.TinyString(t2)
	t3 := "t3"
	t3bc := bcts.TinyString(t3)
	e.Create(id1, &t1bc)
	e.Create(id2, &t2bc)
	e.Create(id3, &t3bc)
	wg.Wait()
	t.Log(str)
	time.Sleep(time.Second * 10)
	if str != "t1t2t3" {
		t.Fail()
	}
}
