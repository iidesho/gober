package competing

import (
	"bytes"
	"context"
	"errors"
	"time"

	log "github.com/cantara/bragi/sbragi"
	"github.com/cantara/gober/consensus"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
	"github.com/cantara/gober/sync"
	"github.com/gofrs/uuid"
)

type timeoutFunk[T any] func(v T) time.Duration

type service[T any] struct {
	stream    consumer.Consumer[tm[T]]
	cryptoKey stream.CryptoKeyProvider
	dataType  string
	//timeout        time.Duration
	selector        uuid.UUID
	completed       sync.SLK
	selectable      chan event.ReadEvent[tm[T]]
	selectedOutput  chan ReadEventWAcc[T]
	completedOutput chan event.ReadEvent[T]
	writeStream     chan event.WriteEventReadStatus[T]
	cons            consensus.Consensus
	timeout         timeoutFunk[T]
	ctx             context.Context
}

type tm[T any] struct {
	Id   uuid.UUID `json:"id"`
	Data T         `json:"data"`
}

func New[T any](s stream.Stream, consBuilder consensus.ConsBuilderFunc, cryptoKey stream.CryptoKeyProvider, from store.StreamPosition, datatype string, timeout timeoutFunk[T], ctx context.Context) (out Consumer[T], err error) {
	fs, err := consumer.New[tm[T]](s, cryptoKey, ctx)
	if err != nil {
		return
	}
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	var t T
	cons, err := consBuilder("competing_"+datatype, timeout(t))
	if err != nil {
		return
	}

	c := service[T]{
		stream:    fs,
		cryptoKey: cryptoKey,
		dataType:  datatype,
		//timeout:        timeoutDuration,
		selector:        name,
		completed:       sync.NewSLK(),
		selectable:      make(chan event.ReadEvent[tm[T]], 100),
		selectedOutput:  make(chan ReadEventWAcc[T], 0),
		completedOutput: make(chan event.ReadEvent[T], 1024),
		writeStream:     make(chan event.WriteEventReadStatus[T], 10),
		cons:            cons,
		timeout:         timeout,
		ctx:             ctx,
	}
	eventStream, err := c.stream.Stream(event.AllTypes(), from, stream.ReadDataType(datatype), c.ctx)
	if err != nil {
		return
	}
	go c.readWrites()
	timeoutChan := make(chan event.ReadEvent[tm[T]], 3)
	go c.readStream(eventStream, timeoutChan)
	timeouts := sync.NewQue[timedat[tm[T]]]()
	go c.timeoutManager(timeouts, timeoutChan)
	go c.timeoutHandler(timeouts)
	go c.selectableHandler(timeoutChan)

	out = &c
	return
}

type timedat[T any] struct {
	e event.ReadEvent[T]
	t time.Time
}

func (c *service[T]) timeoutManager(timeouts sync.Que[timedat[tm[T]]], events <-chan event.ReadEvent[tm[T]]) {
	for e := range events {
		if e.Type == event.Deleted {
			timeouts.Delete(func(v timedat[tm[T]]) bool {
				return bytes.Equal(v.e.Data.Id.Bytes(), e.Data.Id.Bytes())
			})
			continue
		}
		timeouts.Push(timedat[tm[T]]{
			e: e,
			t: time.Now().Add(c.timeout(e.Data.Data)), //c.timeout + time.Millisecond*10), //Adding 10 milliseconds to give some wiggle room with the consesus timouts
		})
	}
}

func (c *service[T]) timeoutHandler(timeouts sync.Que[timedat[tm[T]]]) {
	//timeouts := sync.NewMap[context.CancelFunc]()
	for {
		select {
		case <-timeouts.HasData():
		case <-c.ctx.Done():
			return
		}
		timeout, ok := timeouts.Peek()
		if !ok {
			continue
		}
		select {
		case <-time.After(time.Until(timeout.t)):
			now := time.Now()
			for timeout, ok = timeouts.Peek(); ok && now.After(timeout.t); timeout, ok = timeouts.Peek() {
				timeout, ok = timeouts.Pop()
				if c.completed.Get(timeout.e.Data.Id.String()) {
					log.Trace("timed out a completed event, no need to make it selectable", "id", timeout.e.Data.Id.String())
					continue
				}
				log.Trace("event timed out, writing to compete chan", "id", timeout.e.Data.Id.String())
				c.selectable <- timeout.e
				log.Trace("event timed out, wrote to compete chan", "id", timeout.e.Data.Id.String())
			}
		case <-c.ctx.Done():
			return
		}
	}
}

type seldat[T any] struct {
	e      event.ReadEvent[T]
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *service[T]) selectableHandler(timeout chan<- event.ReadEvent[tm[T]]) {
	selectables := sync.NewQue[event.ReadEvent[tm[T]]]()
	var selected *seldat[tm[T]]
	//ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
	for {
		if selected == nil {
			select {
			case e := <-c.selectable:
				if e.Type == event.Deleted {
					selectables.Delete(func(v event.ReadEvent[tm[T]]) bool {
						return bytes.Equal(e.Data.Id.Bytes(), v.Data.Id.Bytes())
					})
				} else {
					log.Trace("adding selectable event", "id", e.Data.Id.String())
					selectables.Push(e)
				}
			case <-selectables.HasData():
				e, ok := selectables.Pop()
				if !ok {
					continue
				}
				id := e.Data.Id.String()
				if c.completed.Get(id) {
					log.Trace("selectable was already completed and no longer selectable")
					continue
				}
				log.Trace("competing")
				timeout <- e
				won := c.cons.Request(id) //Might need to difirentiate on won lost and completed
				if !won {
					//time.Sleep(time.Second)
					log.Trace("did not win consesus", "id", id)
					continue
				}
				log.Trace("won competition")
				ctx, cancel := context.WithTimeout(c.ctx, c.timeout(e.Data.Data)) //c.timeout)
				selected = &seldat[tm[T]]{
					e:      e,
					ctx:    ctx,
					cancel: cancel,
				}
			case <-c.ctx.Done():
				return
			}
		} else {
			select {
			case e := <-c.selectable:
				if e.Type == event.Deleted {
					if bytes.Equal(selected.e.Data.Id.Bytes(), e.Data.Id.Bytes()) {
						selected = nil
					}
					selectables.Delete(func(v event.ReadEvent[tm[T]]) bool {
						return bytes.Equal(e.Data.Id.Bytes(), v.Data.Id.Bytes())
					})
				} else {
					log.Trace("adding selectable event", "id", e.Data.Id.String())
					selectables.Push(e)
				}
			case <-selected.ctx.Done():
				selected = nil
			case c.selectedOutput <- ReadEventWAcc[T]{
				ReadEvent: event.ReadEvent[T]{
					Event: event.Event[T]{
						Type:     selected.e.Type,
						Data:     selected.e.Data.Data,
						Metadata: selected.e.Metadata,
					},
					Position: selected.e.Position,
					Created:  selected.e.Created,
				},
				CTX: selected.ctx,
				Acc: func(cancel context.CancelFunc, e event.ReadEvent[tm[T]]) func(T) {
					return func(data T) {
						cancel()
						we := event.NewWriteEvent[tm[T]](event.Event[tm[T]]{
							Type: event.Deleted,
							Data: tm[T]{
								Id:   e.Data.Id,
								Data: data,
							}, //e.Data,
							Metadata: e.Metadata,
						})
						c.stream.Write() <- we
						status := <-we.Done()
						if status.Error != nil {
							log.WithError(status.Error).Error("while writing completion event")
						}
						c.cons.Completed(e.Data.Id.String())
					}
				}(selected.cancel, selected.e),
			}:
				selected = nil
			case <-c.ctx.Done():
				return
			}
		}
	}
}

func (c *service[T]) readStream(events <-chan event.ReadEventWAcc[tm[T]], timeout chan<- event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readStream(events, timeout)
		}
	}()

	es := map[string]struct {
		completed bool
		event     event.ReadEvent[tm[T]]
	}{}
	p := uint64(0)
	end, err := c.End()
	log.WithError(err).Trace("got stream end", "end", end, "stream", c.stream.Name())
	for p < end { //catchup loop, read until no more backpressure
		e := <-events
		e.Acc()
		p = e.Position
		switch e.Type {
		case event.Created:
			fallthrough
		case event.Updated:
			id := e.Event.Data.Id.String()
			if !es[id].completed {
				es[id] = struct {
					completed bool
					event     event.ReadEvent[tm[T]]
				}{
					completed: false,
					event:     e.ReadEvent,
				}
			}
		case event.Deleted:
			id := e.Event.Data.Id.String()
			v := es[id]
			v.completed = true
			es[id] = v
		}
	}

	for k, v := range es {
		if v.completed {
			c.completed.Add(k, time.Hour*24*365) //c.timeout*30)
			c.completedOutput <- event.ReadEvent[T]{
				Event: event.Event[T]{
					Type:     v.event.Type,
					Data:     v.event.Data.Data,
					Metadata: v.event.Metadata,
				},
				Position: v.event.Position,
				Created:  v.event.Created,
			}
			continue
		}
		// another dirty hack, should not use go
		//go c.compete(v.event)
		c.selectable <- v.event
		//timeout <- v.event
	}

	log.Trace("backpressure finished")

	for e := range events { //Since this stream is controlled by us, we range over it until it closes
		log.Trace("range read", "event", e.Data.Id.String())
		e.Acc()
		switch e.Type {
		case event.Created:
			fallthrough
		case event.Updated:
			//New event, compete for it
			log.Trace("competing for event", "id", e.Data.Id.String())
			//Move competing to a sepparate thread as it will get stuck and should use a stack or something instead to decide
			//Adding go for now as a work around
			//go c.compete(e.ReadEvent)
		case event.Deleted:
			//This should indicate a finished event. Not sure what to do here.
			log.Trace("read a completed event", "id", e.Data.Id.String())
			c.completed.Add(e.Data.Id.String(), time.Hour*24*365) //c.timeout*30)
			log.Trace("writing to timeout chan")
			timeout <- e.ReadEvent
			log.Trace("wrote to timeout chan")
			c.completedOutput <- event.ReadEvent[T]{
				Event: event.Event[T]{
					Type:     e.Type,
					Data:     e.Data.Data,
					Metadata: e.Metadata,
				},
				Position: e.Position,
				Created:  e.Created,
			}
		}
		c.selectable <- e.ReadEvent
	}
}

func (c *service[T]) readWrites() {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readWrites()
		}
	}()
	for { //Since this is a read strem not controlled by this process we release it on a ctx done
		select {
		case <-c.ctx.Done():
			return
		case we := <-c.writeStream:
			id, err := uuid.NewV7()
			if err != nil {
				we.Close(store.WriteStatus{
					Error: errors.New("could not create new id"),
				})
				return
			}
			//Seems like it should be possible to do away with this map function, if id is required or generated on the event. But then argh
			we.Event().Type = event.Created
			log.Debug("writing mapped event", "id", id)
			if we.Event().Metadata.DataType == "" {
				we.Event().Metadata.DataType = c.dataType
			}
			if we.Event().Metadata.Version == "" {
				we.Event().Metadata.Version = "v0.0.0"
			}
			c.stream.Write() <- event.Map[T, tm[T]](we, func(t T) tm[T] {
				return tm[T]{
					Id:   id,
					Data: t,
				}
			})
			log.Debug("worte mapped event", "id", id)
		}
	}
}

func (c *service[T]) Write() chan<- event.WriteEventReadStatus[T] {
	return c.writeStream
}

func (c *service[T]) Stream() <-chan ReadEventWAcc[T] {
	return c.selectedOutput
}

func (c *service[T]) Completed() <-chan event.ReadEvent[T] {
	return c.completedOutput
}

func (c *service[T]) End() (pos uint64, err error) {
	return c.stream.FilteredEnd(event.AllTypes(), stream.ReadDataType(c.dataType))
}

func (c *service[T]) Name() string {
	return c.stream.Name()
}
