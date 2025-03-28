package contenious_test

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/consensus/contenious"
	"github.com/iidesho/gober/discovery"
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

var (
	c         contenious.Consensus
	aborted   <-chan contenious.ConsID
	approved  <-chan contenious.ConsID
	c2        contenious.Consensus
	aborted2  <-chan contenious.ConsID
	approved2 <-chan contenious.ConsID
	c3        contenious.Consensus
	aborted3  <-chan contenious.ConsID
	approved3 <-chan contenious.ConsID
	s         webserver.Server
	ctx       context.Context
	cancel    context.CancelFunc
)

var (
	id1 = contenious.ConsID(uuid.Must(uuid.NewV7()))
	id2 = contenious.ConsID(uuid.Must(uuid.NewV7()))
	id3 = contenious.ConsID(uuid.Must(uuid.NewV7()))
	d   = local{nodes: []string{"localhost:3132"}}
)

func TestInit(t *testing.T) {
	dl, _ := log.NewLogger(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		AddSource:   true,
		Level:       log.LevelInfo,
		ReplaceAttr: log.ReplaceAttr,
	}))
	dl.SetDefault()
	log.Info("Initializing consensus")
	token := sync.NewObj[string]()
	token.Set("someTestToken")
	topic := "test"
	ctx, cancel = context.WithCancel(context.Background())
	serv, err := webserver.Init(3132, true)
	if err != nil {
		t.Fatal(err)
	}
	c, aborted, approved, err = contenious.New(
		serv,
		token,
		&d,
		topic,
		ctx,
	)
	if err != nil {
		t.Fatal(err)
	}
	s = serv
	go serv.Run()
	time.Sleep(time.Second)
}

func TestConsensusAlone(t *testing.T) {
	c.Request(id3)
	select {
	case id := <-approved:
		if id != id3 {
			t.Fatal("approved id is not id3")
		}
	case <-time.Tick(time.Second * 10):
		t.Fatal("did not win request")
	}
}

func TestInitCluster(t *testing.T) {
	log.Info("Initializing consensus")
	d.nodes = []string{"localhost:3132", "localhost:3133", "localhost:3134"}
	token := sync.NewObj[string]()
	token.Set("someTestToken")
	topic := "test"
	serv2, err := webserver.Init(3133, true)
	if err != nil {
		t.Fatal(err)
	}
	c2, aborted2, approved2, err = contenious.New(
		serv2,
		token,
		New([]string{"localhost:3133", "localhost:3132", "localhost:3134"}),
		topic,
		context.TODO(),
	)
	if err != nil {
		t.Fatal(err)
	}
	serv3, err := webserver.Init(3134, true)
	if err != nil {
		t.Fatal(err)
	}
	c3, aborted3, approved3, err = contenious.New(
		serv3,
		token,
		New([]string{"localhost:3134", "localhost:3133", "localhost:3132"}),
		topic,
		context.TODO(),
	)
	if err != nil {
		t.Fatal(err)
	}
	go serv2.Run()
	go serv3.Run()
	time.Sleep(time.Second)
}

func TestConsensus(t *testing.T) {
	t.Log("requesting id1")
	c.Request(id1)
	t.Log("requeted id1")
	t.Log(c.Consents())
	t.Log(c.Connected())
	t.Log(c2.Consents())
	t.Log(c2.Connected())
	t.Log(c3.Consents())
	t.Log(c3.Connected())
	select {
	case id := <-approved:
		if id != id1 {
			t.Log(c.Consents())
			t.Log(c.Connected())
			t.Log(c2.Consents())
			t.Log(c2.Connected())
			t.Log(c3.Consents())
			t.Log(c3.Connected())
			t.Fatal("approved id is not id1")
		}
	case <-time.Tick(time.Second * 10):
		t.Fatal("did not win request")
	}
}

func TestHasConsentRequest(t *testing.T) {
	c2.Request(id1)
	select {
	case id := <-approved2:
		if id == id1 {
			t.Fatal("did win previously won id1")
		}
		t.Fatal("did win random id")
	case <-time.Tick(time.Second * 1):
	}
}

func TestCompleted(t *testing.T) {
	c.Completed(id1)
	time.Sleep(time.Second)
	c.Abort(id1)
	time.Sleep(time.Second)
	c2.Request(id1)
	select {
	case id := <-approved2:
		if id == id1 {
			t.Fatal("did win previously completed id1")
		}
		t.Fatal("did win random id")
	case <-time.Tick(time.Second * 1):
	}
}

func TestDisconnect(t *testing.T) {
	c.Request(id2)
	time.Sleep(time.Second * 5)
	log.Info(c.Consents()[id2.String()])
	log.Info(c2.Consents()[id2.String()])
	log.Info(c3.Consents()[id2.String()])
	log.Info("cancelling")
	cancel()
	log.Info("cancelled")
	time.Sleep(time.Second * 10)
	log.Info("shutting down")
	s.Shutdown()
	log.Info("shutdown")
	log.Info(c2.Consents()[id2.String()])
	log.Info(c3.Consents()[id2.String()])
	aid3 := <-aborted3
	log.Info("aid3", "id", aid3)
	aid2 := <-aborted2
	log.Info("aid2", "id", aid2)
	if aid3 != aid2 || aid3 != id2 {
		t.Fatalf("aid3 != aid2 != id2: %s != %s != %s", aid3, aid2, id2)
	}
	time.Sleep(time.Second)
	c2.Request(id1)
	c2.Request(id2)
	select {
	case id := <-approved2:
		if id == id1 {
			t.Fatal("did win previously completed id1")
		}
		if id != id2 {
			t.Fatal("did win random id")
		}
	case <-time.Tick(time.Second * 1):
		t.Log(c2.Consents())
		t.Log(c2.Connected())
		t.Log(c3.Consents())
		t.Log(c3.Connected())
		t.Fatal("did not win request")
	}
}

func TestAbort(t *testing.T) {
	c2.Abort(id2)
	aid1 := <-aborted2
	aid2 := <-aborted3
	if aid1 != aid2 || aid1 != id2 {
		t.Fatalf("aid1 != aid2 != id2: %s != %s != %s", aid1, aid2, id2)
	}
	time.Sleep(time.Second)
	c3.Request(id2)
	t.Log(c3.Consents())
	select {
	case id := <-approved3:
		if id != id2 {
			t.Fatal("did win random id")
		}
	case <-time.Tick(time.Second * 1):
		t.Log(c2.Consents())
		t.Log(c2.Connected())
		t.Log(c3.Consents())
		t.Log(c3.Connected())
		t.Fatal("did not win request")
	}
	t.Log(c2.Consents())
	t.Log(c3.Consents())
}

/*
func TestCompletion(t *testing.T) {
	if !c.Request("id3") {
		t.Fatal("did not win request")
	}
	c.Completed("id3")
	if c.Request("id3") {
		t.Fatal("did win request after completion")
	}
	time.Sleep(time.Second)
	if c.Request("id3") {
		t.Fatal("did win request after completion and timeout")
	}
}
*/
