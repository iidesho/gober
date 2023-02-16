package competing

import (
	"bytes"
	"context"
	"errors"
	log "github.com/cantara/bragi"
	"github.com/cantara/gober/stream"
	"github.com/cantara/gober/stream/consumer"
	"github.com/cantara/gober/stream/event"
	"github.com/cantara/gober/stream/event/store"
	"github.com/gofrs/uuid"
	"time"
)

type competing[T any] struct {
	stream         stream.FilteredStream
	cryptoKey      stream.CryptoKeyProvider
	selected       consumer.Map[event.ReadEvent[tm[T]]]
	finished       consumer.Map[struct{}]
	timeout        time.Duration
	selector       uuid.UUID
	selectedOutput chan event.ReadEventWAcc[T]
	writeStream    chan event.WriteEvent[T]
	ctx            context.Context
}

type tm[T any] struct {
	Id       uuid.UUID `json:"id"`
	Data     T         `json:"data"`
	Timeout  time.Time `json:"timeout"`
	Selector uuid.UUID `json:"selector"`
}

func New[T any](s stream.FilteredStream, cryptoKey stream.CryptoKeyProvider, from store.StreamPosition, datatype string, timeoutDuration time.Duration, ctx context.Context) (out Consumer[T], err error) {
	name, err := uuid.NewV7()
	if err != nil {
		return
	}
	c := competing[T]{
		stream:         s,
		cryptoKey:      cryptoKey,
		selected:       consumer.NewMap[event.ReadEvent[tm[T]]](),
		finished:       consumer.NewMap[struct{}](),
		timeout:        timeoutDuration,
		selector:       name,
		selectedOutput: make(chan event.ReadEventWAcc[T], 0),
		writeStream:    make(chan event.WriteEvent[T], 10),
		ctx:            ctx,
	}
	eventStream, err := c.stream.Stream(event.AllTypes(), from, stream.ReadDataType(datatype), c.ctx)
	if err != nil {
		return
	}

	selectable, timeout, finished, selected := c.startReadStream(eventStream)
	go c.readSelectables(selectable)
	go c.readTimeout(timeout, selectable)
	go c.readFinished(finished)

	go c.readSelected(selected, c.selectedOutput)
	go c.readWrites(c.writeStream)

	out = &c
	return
}

func (c *competing[T]) Write() chan<- event.WriteEvent[T] {
	return c.writeStream
}

func (c *competing[T]) Stream() <-chan event.ReadEventWAcc[T] {
	return c.selectedOutput
}

func (c *competing[T]) readSelectables(selectable <-chan event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Crit("Recovered ", r)
			c.readSelectables(selectable)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case e := <-selectable: //This needs some kind of throttling
			if e.Metadata.Created.Add(30 * time.Second).Before(time.Now()) {
				go func() {
					time.Sleep(time.Minute)
					if _, isFinished := c.finished.Load(e.Data.Id.String()); isFinished {
						return
					}
					selected, isSelected := c.selected.Load(e.Data.Id.String())
					if isSelected && selected.Data.Timeout.After(time.Now()) {
						return
					}
					c.compete(e.Event)
				}()
				continue
			}
			c.compete(e.Event)
		}
	}
}

func (c *competing[T]) readTimeout(timeout <-chan event.ReadEvent[tm[T]], selectable chan<- event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Crit("Recovered ", r)
			c.readTimeout(timeout, selectable)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case newTimeout := <-timeout:
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Crit("Hid panic ", r)
					}
				}()
				id := newTimeout.Data.Id
				if newTimeout.Metadata.Created.Add(30 * time.Second).Before(time.Now()) { //This needs to be verified.
					time.Sleep(time.Minute)
					if _, isFinished := c.finished.Load(id.String()); isFinished {
						return
					}
					selected, isSelected := c.selected.Load(id.String())
					if isSelected && selected.Data.Timeout.After(time.Now()) {
						return
					}
				}
				select {
				case <-c.ctx.Done():
					return
				case <-time.After(newTimeout.Data.Timeout.Sub(time.Now())):
					if _, finished := c.finished.Load(id.String()); finished {
						return
					}
					selected, ok := c.selected.Load(id.String())
					if !ok {
						log.Error("This should never be true, neither finished nor selected but timed out ", id)
						return
					}
					if !selected.Metadata.Created.Equal(newTimeout.Metadata.Created) {
						return
					}
					//log.Debug("TIMED OUT!! ", id)
					selectable <- newTimeout
				}
			}()
		}
	}
}

func (c *competing[T]) readSelected(selected <-chan event.ReadEvent[tm[T]], out chan<- event.ReadEventWAcc[T]) {
	defer func() {
		if r := recover(); r != nil {
			log.Crit("Recovered ", r)
			c.readSelected(selected, out)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			close(out)
			return
		case current := <-selected:
			currentCTX, cancel := context.WithTimeout(c.ctx, c.timeout-time.Now().Sub(current.Metadata.Created)-(time.Second*5))
			select {
			case <-c.ctx.Done():
				return
			case <-currentCTX.Done():
				//log.Debug("write timeout ", current.Data.ID)
			case out <- event.ReadEventWAcc[T]{
				ReadEvent: event.ReadEvent[T]{
					Event: event.Event[T]{
						Type:     current.Type,
						Data:     current.Data.Data,
						Metadata: current.Metadata,
					},
					Position: current.Position,
				},
				Acc: func() {
					defer cancel()

					bes, err := consumer.EncryptEvent[tm[T]](event.Event[tm[T]]{
						Type:     event.Delete,
						Data:     current.Data,
						Metadata: current.Metadata,
					}, c.cryptoKey)
					if err != nil {
						log.AddError(err).Error("while encrypting acc event")
						return
					}
					c.stream.Write() <- event.ByteWriteEvent{
						Event: event.Event[[]byte](bes),
					}
					/*
						if err != nil {
							log.AddError(err).Error("while storing finished competing consumer")
							return
						}
					*/
				},
				CTX: currentCTX,
			}:
				//log.Debug("wrote ", current.Data.ID)
			}
		}
	}
}

func (c *competing[T]) readFinished(finished <-chan event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Crit("Recovered ", r)
			c.readFinished(finished)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-finished: //Not sure what to do here
		}
	}
}

func (c *competing[T]) startReadStream(eventStream <-chan event.ByteReadEvent) (selectable chan event.ReadEvent[tm[T]], timeout, finished <-chan event.ReadEvent[tm[T]], selected <-chan event.ReadEvent[tm[T]]) {
	selectableChan := make(chan event.ReadEvent[tm[T]], 10)
	timeoutChan := make(chan event.ReadEvent[tm[T]], 10)
	finishedChan := make(chan event.ReadEvent[tm[T]], 10)
	selectedChan := make(chan event.ReadEvent[tm[T]], 10)

	go c.readStream(eventStream, selectableChan, timeoutChan, finishedChan, selectedChan)

	selectable = selectableChan
	selected = selectedChan
	timeout = timeoutChan
	finished = finishedChan
	return
}

func (c *competing[T]) readStream(eventStream <-chan event.ByteReadEvent, selectable, timeout, finished chan<- event.ReadEvent[tm[T]], selected chan<- event.ReadEvent[tm[T]]) {
	defer func() {
		if r := recover(); r != nil {
			log.Crit("Recovered ", r)
			c.readStream(eventStream, selectable, timeout, finished, selected)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			close(selectable)
			close(timeout)
			close(finished)
			close(selected)
			log.Debug("CLOSED CHANNELS !!!!")
			return
		case ce := <-eventStream:
			e, err := consumer.DecryptEvent[tm[T]](ce, c.cryptoKey)
			if err != nil {
				log.AddError(err).Error("while reading event")
				continue
			}
			id := e.Data.Id
			if e.Type == event.Delete {
				c.finished.Store(id.String(), struct{}{})
				finished <- e //Delete from selected
				continue
			}
			_, isFinished := c.finished.Load(id.String())
			if isFinished {
				log.Debug("skipping event since it is finished, ", id)
				continue
			}
			if e.Type == event.Create {
				selectable <- e
				continue
			}
			if time.Now().After(e.Data.Timeout) {
				//If this is a new server and catchup is too slow, the event could in theory time out before we get back to it.
				//  Currently considering that highly unlikely.
				continue
			}
			stored, isSelected := c.selected.Load(id.String())
			if isSelected && !e.Metadata.Created.After(stored.Data.Timeout) {
				continue
			}
			//log.Debug("storing ", e.Data.Id, " in runner ", c.selector, " with winner ", e.Data.Selector)
			c.selected.Store(id.String(), e)
			timeout <- e
			//log.Println(e.Data.Selector, " ", c.selector, " ", e.Data.Id)
			if bytes.Equal(e.Data.Selector.Bytes(), c.selector.Bytes()) {
				///log.Println(e.Data.Selector, " == ", c.selector)
				selected <- e
				continue
			}
		}
	}
}

func (c *competing[T]) readWrites(events <-chan event.WriteEvent[T]) {
	defer func() {
		if r := recover(); r != nil {
			log.Crit("Recovered ", r)
			c.readWrites(events)
		}
	}()
	for {
		select {
		case <-c.ctx.Done():
			return
		case e := <-events:
			if e.Type != "" && e.Type != event.Create {
				if e.Status != nil {
					e.Status <- event.WriteStatus{
						Error: errors.New("event type should always be create or empty when using competing consumers"),
					}
				}
				return
			}
			id, err := uuid.NewV7()
			if err != nil {
				return
			}
			es := event.Event[tm[T]]{
				Type: event.Create,
				Data: tm[T]{
					Id:   id,
					Data: e.Data,
				},
				Metadata: e.Metadata,
			}
			bes, err := consumer.EncryptEvent[tm[T]](es, c.cryptoKey)
			if err != nil {
				if e.Status != nil {
					e.Status <- event.WriteStatus{
						Error: err,
					}
				}
				return
			}
			c.stream.Write() <- event.ByteWriteEvent{
				Event:  event.Event[[]byte](bes),
				Status: e.Status,
			}
		}
	}
}

func (c *competing[T]) End() (pos uint64, err error) {
	return c.stream.End()
}

func (c *competing[T]) Name() string {
	return c.stream.Name()
}

func (c *competing[T]) compete(e event.Event[tm[T]]) {
	e.Type = event.Update
	e.Data.Selector = c.selector
	e.Data.Timeout = time.Now().Add(c.timeout)
	es, err := consumer.EncryptEvent[tm[T]](e, c.cryptoKey)
	if err != nil {
		log.AddError(err).Error("while encrypting during compete")
		return
	}
	//log.Debug("competing on ", e.Data.Id, " with runner ", c.selector)

	_, err = c.stream.Store(es)
	if err != nil {
		log.AddError(err).Warning("while trying to compete")
		return
	}
}
