package main

import (
	"context"
	"flag"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store/inmemory"
	"github.com/iidesho/gober/stream/event/store/ondisk"
)

var (
	STREAM_NAME  = "BenchmarkConsumer_" + uuid.Must(uuid.NewV7()).String()
	testCryptKey = "aPSIX6K3yw6cAWDQHGPjmhuOswuRibjyLLnd91ojdK0="
)

var (
	size      int
	eventSize int
	inmem     bool
)

func init() {
	const ( // TODO: Add bootstrap git as a separate command from bootstrap.
		defaultInmem          = false
		inmemUsage            = "use in memory store"
		defaultNumberOfEvents = 10000000
		numberOfEventsUsage   = "sets the number of events to create and write"
		defaultEventSize      = 1000
		eventSizeUsage        = "event size in Bytes"
	)
	flag.BoolVar(&inmem, "bootstrap", defaultInmem, inmemUsage)
	flag.BoolVar(&inmem, "b", defaultInmem, inmemUsage+" (shorthand)")
	flag.IntVar(&size, "num", defaultNumberOfEvents, numberOfEventsUsage)
	flag.IntVar(&size, "n", defaultNumberOfEvents, numberOfEventsUsage+" (shorthand)")
	flag.IntVar(&eventSize, "size", defaultEventSize, eventSizeUsage)
	flag.IntVar(&eventSize, "s", defaultEventSize, eventSizeUsage+" (shorthand)")
}

func main() {
	flag.Parse()
	// log.SetLevel(log.ERROR) TODO: should add to sbragi
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var pers stream.Stream
	var err error
	if inmem {
		pers, err = inmemory.Init(STREAM_NAME, ctx)
		if err != nil {
			log.WithError(err).Fatal("while connecting to storage")
			return
		}
	} else {
		pers, err = ondisk.Init(STREAM_NAME, ctx)
		if err != nil {
			log.WithError(err).Fatal("while connecting to storage")
			return
		}
	}
	c, err := stream.Init[bcts.SmallBytes](pers, ctx)
	if err != nil {
		log.WithError(err).Fatal("while creating consumer")
		return
	}
	/*
		readEventStream, err := c.Stream(event.AllTypes(), store.STREAM_START, stream.ReadAll(), ctx)
		if err != nil {
			log.WithError(err).Fatal("while starting stream")
			return
		}
		events := make([]event.WriteEventReadStatus[[]byte], size)
		go func() {
			for i := 0; i < size; i++ {
				e := <-readEventStream
				e.Acc()
				if e.Type != event.Create {
					log.Fatal("missmatch event types")
					return
				}
				/*
					if e.Data.Id != events[i].Event().Data.Id {
						b.Error(fmt.Errorf("missmatch event data, %v != %v", e.Data, events[i].Event().Data))
						return
					}
				/
			}
		}()
	*/
	defer func(start time.Time) {
		fin := time.Now()
		dur := fin.Sub(start)
		eps := float64(size) / float64(dur/time.Second)
		mbps := eps * float64(eventSize) / 1000000
		log.Info(
			"Finished writing events",
			"number of events",
			size,
			"start time",
			start,
			"end time",
			fin,
			"duration",
			dur,
			"event size (B)",
			eventSize,
			"events / second",
			eps,
			"MB/s",
			mbps,
		)
	}(time.Now())
	// writeEventStream := c.Write()
	b := bcts.SmallBytes(make([]byte, eventSize))
	we := event.NewWriteEvent(event.Event[bcts.SmallBytes, *bcts.SmallBytes]{
		Type: event.Created,
		Data: &b,
		Metadata: event.Metadata{
			Extra: map[bcts.TinyString]bcts.SmallBytes{"extra": []byte("extra metadata test")},
		},
	})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < size; i++ {
			/*
				data := dd{
					Id:   i,
					Name: "test" + b.Name(),
				}
			*/
			//events[i] = we
			c.Write() <- we // events[i]
			//<-events[i].Done()
		}
	}()
	wg.Wait()
}
