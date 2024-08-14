package event

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
	"github.com/iidesho/gober/stream/event/store"
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigDefault

type Metadata struct {
	Created   time.Time                           `json:"created"`
	Extra     map[bcts.TinyString]bcts.SmallBytes `json:"extra"`
	Stream    string                              `json:"stream"`
	EventType Type                                `json:"event_type"`
	Version   string                              `json:"version"`
	DataType  string                              `json:"data_type"`
	Key       string                              `json:"key"`
}

func (m Metadata) WriteBytes(w *bufio.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0))
	if err != nil {
		return
	}
	err = bcts.WriteTime(w, m.Created)
	if err != nil {
		return err
	}
	err = bcts.WriteMap(w, m.Extra)
	if err != nil {
		return err
	}
	err = bcts.WriteString(w, m.Stream)
	if err != nil {
		return err
	}
	err = bcts.WriteString(w, string(m.EventType))
	if err != nil {
		return err
	}
	err = bcts.WriteString(w, m.Version)
	if err != nil {
		return err
	}
	err = bcts.WriteString(w, m.DataType)
	if err != nil {
		return err
	}
	err = bcts.WriteString(w, m.Key)
	if err != nil {
		return err
	}
	return w.Flush()
}

func (m *Metadata) ReadBytes(r io.Reader) (err error) {
	var v uint8
	err = bcts.ReadUInt8(r, &v)
	if err != nil {
		return
	}
	if v != 0 {
		return fmt.Errorf("invalid event version, %s=%d, %s=%d", "expected", 0, "got", v)
	}
	err = bcts.ReadTime(r, &m.Created)
	if err != nil {
		return err
	}
	err = bcts.ReadMap(r, &m.Extra)
	if err != nil {
		return err
	}
	err = bcts.ReadString(r, &m.Stream)
	if err != nil {
		return err
	}
	var eventType string
	err = bcts.ReadString(r, &eventType)
	if err != nil {
		return err
	}
	m.EventType = Type(eventType)
	err = bcts.ReadString(r, &m.Version)
	if err != nil {
		return err
	}
	err = bcts.ReadString(r, &m.DataType)
	if err != nil {
		return err
	}
	err = bcts.ReadString(r, &m.Key)
	if err != nil {
		return err
	}
	return nil
}

type Event[T any] struct {
	Metadata Metadata `json:"metadata"`
	Data     T        `json:"data"`
	Type     Type     `json:"type"`
}

type ReadEvent[T any] struct {
	Created time.Time `json:"created"`
	Event[T]
	Position uint64 `json:"position"`
}

type ReadEventWAcc[T any] struct {
	CTX context.Context
	Acc func()
	ReadEvent[T]
}

type WriteEvent[T any] struct {
	status chan store.WriteStatus
	event  Event[T]
}

type WriteEventReadStatus[T any] interface {
	Event() *Event[T]
	Done() <-chan store.WriteStatus
	Close(store.WriteStatus)
	Store() *store.WriteEvent
	StatusChan() chan store.WriteStatus
}

func Map[OT, NT any](e WriteEventReadStatus[OT], f func(OT) NT) WriteEventReadStatus[NT] {
	return &WriteEvent[NT]{
		event: Event[NT]{
			Type:     e.Event().Type,
			Data:     f(e.Event().Data),
			Metadata: e.Event().Metadata,
		},
		status: e.StatusChan(),
	}
}

func NewWriteEvent[T any](e Event[T]) WriteEventReadStatus[T] { // Dont think i like this
	return &WriteEvent[T]{
		event:  e,
		status: make(chan store.WriteStatus, 1),
	}
}

func (e *WriteEvent[T]) Event() *Event[T] {
	return &e.event
}

func (e *WriteEvent[T]) Done() <-chan store.WriteStatus {
	return e.status
}

func (e *WriteEvent[T]) Close(status store.WriteStatus) {
	if e.status == nil {
		return
	}
	e.status <- status
	close(e.status)
}

func (e *WriteEvent[T]) StatusChan() chan store.WriteStatus {
	return e.status
}

func (e *WriteEvent[T]) Store() *store.WriteEvent {
	/*
		mByte, err := json.Marshal(e.event.Metadata)
		if err != nil {
			log.WithError(err).Error("while marshaling metadata")
			e.Close(store.WriteStatus{
				Error: err,
			})
			return nil
		}
	*/
	mByte := bytes.NewBuffer([]byte{})
	err := e.event.Metadata.WriteBytes(bufio.NewWriter(mByte))
	if err != nil {
		log.WithError(err).Error("while marshaling metadata")
		e.Close(store.WriteStatus{
			Error: err,
		})
		return nil
	}

	dByte, err := json.Marshal(e.event.Data)
	if err != nil {
		log.WithError(err).Error("while marshaling data")
		e.Close(store.WriteStatus{
			Error: err,
		})
		return nil
	}
	return &store.WriteEvent{
		Event: store.Event{
			Type:     string(e.event.Type),
			Data:     dByte,
			Metadata: mByte.Bytes(),
		},
		Status: e.status,
	}
}

type (
	ByteEvent      Event[[]byte]
	ByteWriteEvent WriteEvent[[]byte]
	ByteReadEvent  ReadEvent[[]byte]
)
