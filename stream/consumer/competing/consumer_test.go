package competing_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"

	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/consensus"
	"github.com/iidesho/gober/discovery/local"
	"github.com/iidesho/gober/stream/consumer/competing"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/iidesho/gober/stream/event/store/ondisk"
)

var (
	c               competing.Consumer[dd]
	ctxGlobal       context.Context
	ctxGlobalCancel context.CancelFunc
	testCryptKey    = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
	events          = make(map[int]competing.ReadEventWAcc[dd])
)

var STREAM_NAME = "TestCompetingConsumer_" + uuid.Must(uuid.NewV7()).String()

type md struct {
	Extra bcts.SmallBytes `json:"extra"`
}

type dd struct {
	Name string `json:"name"`
	Id   int    `json:"id"`
}

func cryptKeyProvider(_ string) string {
	return testCryptKey
}

func TestInit(t *testing.T) {
	// dl, _ := log.NewDebugLogger()
	// dl.SetDefault()
	ctxGlobal, ctxGlobalCancel = context.WithCancel(context.Background())
	pers, err := ondisk.Init(STREAM_NAME, ctxGlobal)
	if err != nil {
		t.Error(err)
		return
	}

	token := "someTestToken"
	p, err := consensus.Init(3133, token, local.New())
	if err != nil {
		t.Fatal(err)
	}
	c, err = competing.New[dd](
		pers,
		p.AddTopic,
		cryptKeyProvider,
		store.STREAM_START,
		"datatype",
		func(v dd) time.Duration {
			if v.Id == 0 {
				return time.Microsecond * 500
			}
			if v.Id == 10 {
				return time.Second * 3
			}
			return time.Second * 1
		},
		ctxGlobal,
	)
	if err != nil {
		t.Error(err)
		return
	}
	go p.Run()
	time.Sleep(time.Microsecond)
	return
}

func TestStoreOrder(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 1; i <= 5; i++ {
		log.Info("loop start", "id", i)
		data := dd{
			Id:   i,
			Name: "test",
		}
		meta := md{
			Extra: bcts.SmallBytes("extra metadata test"),
		}
		e := event.Event[dd]{
			Type: event.Created,
			Data: data,
			Metadata: event.Metadata{
				Extra:    map[bcts.TinyString]bcts.SmallBytes{"extra": meta.Extra},
				DataType: "datatype",
			},
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log.Info("reading event")
			read := <-c.Stream()
			log.Info("read event", "event", read.Data.Id)
			events[i] = read
			read.Acc(read.Data)
			log.Info("acced read event")
		}(i)

		we := event.NewWriteEvent(e)
		log.Info("writing event", "id", i)
		c.Write() <- we
		status := <-we.Done()
		if status.Error != nil {
			t.Error(status.Error)
			return
		}
		log.Info("wrote event", "id", i)
		/*
			_, err := c.Store(e)
			if err != nil {
				t.Error(err)
				return
			}
		*/
		log.Info("loop end", "id", i)
	}
	log.Info("waiting for event to read")
	wg.Wait()
	return
}

func TestStreamOrder(t *testing.T) {
	for i := 1; i <= 5; i++ {
		e := events[i]
		log.Info("events", "events", e)
		if e.Data.Id != i {
			t.Error(fmt.Errorf("missmatch event data id: %d != %d", e.Data.Id, i))
			return
		}
		if e.Data.Name != "test" {
			t.Error(fmt.Errorf("missmatch event data name"))
			return
		}
		if string(e.Metadata.Extra["extra"]) != "extra metadata test" {
			t.Error(fmt.Errorf("missmatch event metadata extra"))
			return
		}
		if e.Metadata.EventType != e.Type {
			t.Error(fmt.Errorf("missmatch event metadata type and event type"))
			return
		}
	}
	return
}

func TestTimeout(t *testing.T) {
	data := dd{
		Id:   10,
		Name: "test_timeout",
	}

	e := event.Event[dd]{
		Type: event.Created,
		Data: data,
		Metadata: event.Metadata{
			DataType: "datatype",
		},
	}

	we := event.NewWriteEvent(e)
	log.Info("writing event to timeout")
	c.Write() <- we
	status := <-we.Done()
	if status.Error != nil {
		t.Error(status.Error)
		return
	}
	log.Info("wrote event to timeout")
	/*
		c.Write() <- event.WriteEvent[dd]{
			Event: e,
		}
			_, err := c.Store(e)
			if err != nil {
				t.Error(err)
				return
			}
	*/
	log.Info("reading event to discard")
	read := <-c.Stream()
	log.Info("read", "event", read)
	log.Info("waiting until after timeout (5s)")
	time.Sleep(time.Second * 5)
	// log.Info("accing event after timeout and it should have been discarded") //This and the next one should probably not be true anymore :/
	// read.Acc()

	log.Info("reading event to acc")
	select {
	case read = <-c.Stream():
	case <-time.After(5 * time.Second):
		// default:
		t.Error("task was not ready to select")
		return
	}
	log.Info("read", "event", read)
	read.Acc(read.Data)
	log.Info("verifying there is no extra events a peering (9s)")
	select {
	case <-time.After(9 * time.Second):
	case read = <-c.Stream():
		t.Error("task still existed after timeout: ", read)
		return
	}
}

func TestTairdown(t *testing.T) {
	ctxGlobalCancel()
}
