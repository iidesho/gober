package competing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gofrs/uuid"
	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/consensus"
	"github.com/iidesho/gober/stream"
	"github.com/iidesho/gober/stream/consumer"
	"github.com/iidesho/gober/stream/event"
	"github.com/iidesho/gober/stream/event/store"
	"github.com/iidesho/gober/sync"
)

type timeoutFunk[BT any, T bcts.ReadWriter[BT]] func(v T) time.Duration

type service[BT any, T bcts.ReadWriter[BT]] struct {
	stream          consumer.Consumer[tm[BT, T], *tm[BT, T]]
	completed       sync.SLK
	cons            consensus.Consensus
	ctx             context.Context
	cryptoKey       stream.CryptoKeyProvider
	selectable      chan event.ReadEvent[tm[BT, T], *tm[BT, T]]
	selectedOutput  chan ReadEventWAcc[BT, T]
	completedOutput chan event.ReadEvent[BT, T]
	writeStream     chan event.WriteEventReadStatus[BT, T]
	timeout         timeoutFunk[BT, T]
	dataType        string
	selector        uuid.UUID
}

type tm[BT any, T bcts.ReadWriter[BT]] struct {
	Data T         `json:"data"`
	Id   uuid.UUID `json:"id"`
}

func (s *tm[BT, T]) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0)) //Version
	if err != nil {
		return
	}
	err = bcts.WriteStaticBytes(w, s.Id[:])
	if err != nil {
		return
	}
	err = s.Data.WriteBytes(w)
	if err != nil {
		return
	}
	return nil
}

func (s *tm[BT, T]) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid tm version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	err = bcts.ReadStaticBytes(r, s.Id[:])
	if err != nil {
		return
	}
	return nil
}

func New[BT any, T bcts.ReadWriter[BT]](
	s stream.Stream,
	consBuilder consensus.ConsBuilderFunc,
	cryptoKey stream.CryptoKeyProvider,
	from store.StreamPosition,
	datatype string,
	timeout timeoutFunk[BT, T],
	ctx context.Context,
) (out Consumer[BT, T], err error) {
	fs, err := consumer.New[tm[BT, T], *tm[BT, T]](s, cryptoKey, ctx)
	if err != nil {
		return
	}
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	var t BT
	cons, err := consBuilder("competing_"+datatype, timeout(&t))
	if err != nil {
		return
	}

	c := service[BT, T]{
		stream:    fs,
		cryptoKey: cryptoKey,
		dataType:  datatype,
		//timeout:        timeoutDuration,
		selector:        name,
		completed:       sync.NewSLK(),
		selectable:      make(chan event.ReadEvent[tm[BT, T], *tm[BT, T]], 100),
		selectedOutput:  make(chan ReadEventWAcc[BT, T], 0),
		completedOutput: make(chan event.ReadEvent[BT, T], 1024),
		writeStream:     make(chan event.WriteEventReadStatus[BT, T], 10),
		cons:            cons,
		timeout:         timeout,
		ctx:             ctx,
	}
	eventStream, err := c.stream.Stream(
		event.AllTypes(),
		from,
		stream.ReadDataType(datatype),
		c.ctx,
	)
	if err != nil {
		return
	}
	go c.readWrites()
	timeoutChan := make(chan event.ReadEvent[tm[BT, T], *tm[BT, T]], 3)
	go c.readStream(eventStream, timeoutChan)
	timeouts := sync.NewQue[timedat[tm[BT, T], *tm[BT, T]], *timedat[tm[BT, T], *tm[BT, T]]]()
	go c.timeoutManager(timeouts, timeoutChan)
	go c.timeoutHandler(timeouts)
	go c.selectableHandler(timeoutChan)

	out = &c
	return
}

type timedat[BT any, T bcts.ReadWriter[BT]] struct {
	t time.Time
	e event.ReadEvent[BT, T]
}

func (s *timedat[BT, T]) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0)) //Version
	if err != nil {
		return
	}
	err = bcts.WriteTime(w, s.t)
	if err != nil {
		return
	}
	return nil
}

func (s *timedat[BT, T]) ReadBytes(r io.Reader) (err error) {
	var vers uint8
	err = bcts.ReadUInt8(r, &vers)
	if err != nil {
		return
	}
	if vers != 0 {
		return fmt.Errorf("invalid timedat version, %s=%d, %s=%d", "expected", 0, "got", vers)
	}
	err = bcts.ReadTime(r, &s.t)
	if err != nil {
		return
	}
	return nil
}

func (c *service[BT, T]) timeoutManager(
	timeouts *sync.Que[timedat[tm[BT, T], *tm[BT, T]], *timedat[tm[BT, T], *tm[BT, T]]],
	events <-chan event.ReadEvent[tm[BT, T], *tm[BT, T]],
) {
	for e := range events {
		if e.Type == event.Deleted {
			timeouts.Delete(func(v *timedat[tm[BT, T], *tm[BT, T]]) bool {
				return bytes.Equal(v.e.Data.Id.Bytes(), e.Data.Id.Bytes())
			})
			continue
		}
		timeouts.Push(&timedat[tm[BT, T], *tm[BT, T]]{
			e: e,
			t: time.Now().
				Add(c.timeout(e.Data.Data)),
			//c.timeout + time.Millisecond*10), //Adding 10 milliseconds to give some wiggle room with the consesus timouts
		})
	}
}

func (c *service[BT, T]) timeoutHandler(
	timeouts *sync.Que[timedat[tm[BT, T], *tm[BT, T]], *timedat[tm[BT, T], *tm[BT, T]]],
) {
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
					log.Trace(
						"timed out a completed event, no need to make it selectable",
						"id",
						timeout.e.Data.Id.String(),
					)
					continue
				}
				log.Trace(
					"event timed out, writing to compete chan",
					"id",
					timeout.e.Data.Id.String(),
				)
				c.selectable <- timeout.e
				log.Trace(
					"event timed out, wrote to compete chan",
					"id",
					timeout.e.Data.Id.String(),
				)
			}
		case <-c.ctx.Done():
			return
		}
	}
}

type seldat[BT any, T bcts.ReadWriter[BT]] struct {
	ctx    context.Context
	cancel context.CancelFunc
	e      event.ReadEvent[BT, T]
}

func (c *service[BT, T]) selectableHandler(timeout chan<- event.ReadEvent[tm[BT, T], *tm[BT, T]]) {
	selectables := sync.NewQue[event.ReadEvent[tm[BT, T], *tm[BT, T]]]()
	var selected *seldat[tm[BT, T], *tm[BT, T]]
	//ctx, cancel := context.WithTimeout(c.ctx, c.timeout)
	for {
		if selected == nil {
			select {
			case e := <-c.selectable:
				if e.Type == event.Deleted {
					selectables.Delete(func(v *event.ReadEvent[tm[BT, T], *tm[BT, T]]) bool {
						return bytes.Equal(e.Data.Id.Bytes(), v.Data.Id.Bytes())
					})
				} else {
					log.Trace("adding selectable event", "id", e.Data.Id.String())
					selectables.Push(&e)
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
				timeout <- *e
				won := c.cons.Request(id) //Might need to difirentiate on won lost and completed
				if !won {
					//time.Sleep(time.Second)
					log.Trace("did not win consesus", "id", id)
					continue
				}
				log.Trace("won competition")
				ctx, cancel := context.WithTimeout(c.ctx, c.timeout(e.Data.Data)) //c.timeout)
				selected = &seldat[tm[BT, T], *tm[BT, T]]{
					e:      *e,
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
					selectables.Delete(func(v *event.ReadEvent[tm[BT, T], *tm[BT, T]]) bool {
						return bytes.Equal(e.Data.Id.Bytes(), v.Data.Id.Bytes())
					})
				} else {
					log.Trace("adding selectable event", "id", e.Data.Id.String())
					selectables.Push(&e)
				}
			case <-selected.ctx.Done():
				selected = nil
			case c.selectedOutput <- ReadEventWAcc[BT, T]{
				ReadEvent: event.ReadEvent[BT, T]{
					Event: event.Event[BT, T]{
						Type:     selected.e.Type,
						Data:     selected.e.Data.Data,
						Metadata: selected.e.Metadata,
					},
					Position: selected.e.Position,
					Created:  selected.e.Created,
				},
				CTX: selected.ctx,
				Acc: func(cancel context.CancelFunc, e event.ReadEvent[tm[BT, T], *tm[BT, T]]) func(T) {
					return func(data T) {
						cancel()
						we := event.NewWriteEvent[tm[BT, T]](event.Event[tm[BT, T], *tm[BT, T]]{
							Type: event.Deleted,
							Data: &tm[BT, T]{
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

func (c *service[BT, T]) readStream(
	events <-chan event.ReadEventWAcc[tm[BT, T], *tm[BT, T]],
	timeout chan<- event.ReadEvent[tm[BT, T], *tm[BT, T]],
) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("recovered", "error", r)
			c.readStream(events, timeout)
		}
	}()

	es := map[string]struct {
		event     event.ReadEvent[tm[BT, T], *tm[BT, T]]
		completed bool
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
					event     event.ReadEvent[tm[BT, T], *tm[BT, T]]
					completed bool
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
			c.completedOutput <- event.ReadEvent[BT, T]{
				Event: event.Event[BT, T]{
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
			c.completedOutput <- event.ReadEvent[BT, T]{
				Event: event.Event[BT, T]{
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

func (c *service[BT, T]) readWrites() {
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
			c.stream.Write() <- event.Map(we, func(t T) *tm[BT, T] {
				return &tm[BT, T]{
					Id:   id,
					Data: t,
				}
			})
			log.Debug("worte mapped event", "id", id)
		}
	}
}

func (c *service[BT, T]) Write() chan<- event.WriteEventReadStatus[BT, T] {
	return c.writeStream
}

func (c *service[BT, T]) Stream() <-chan ReadEventWAcc[BT, T] {
	return c.selectedOutput
}

func (c *service[BT, T]) Completed() <-chan event.ReadEvent[BT, T] {
	return c.completedOutput
}

func (c *service[BT, T]) End() (pos uint64, err error) {
	return c.stream.FilteredEnd(event.AllTypes(), stream.ReadDataType(c.dataType))
}

func (c *service[BT, T]) Name() string {
	return c.stream.Name()
}
