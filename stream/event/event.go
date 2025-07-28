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
	// jsoniter "github.com/json-iterator/go"
)

// var json = jsoniter.ConfigDefault

type Metadata struct {
	Created   time.Time         `json:"created"`
	Extra     map[string]string `json:"extra"`
	Stream    string            `json:"stream"`
	EventType Type              `json:"event_type"`
	Version   string            `json:"version"`
	DataType  string            `json:"data_type"`
	Key       string            `json:"key"`
}

func (m Metadata) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0))
	if err != nil {
		return
	}
	err = bcts.WriteTime(w, m.Created)
	if err != nil {
		return err
	}
	err = bcts.WriteMapAny(w, m.Extra, bcts.WriteTinyString, bcts.WriteSmallString)
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
	return nil
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
	err = bcts.ReadMapAny(r, &m.Extra, bcts.ReadTinyString, bcts.ReadSmallString)
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

type Event[BT any, T bcts.ReadWriter[BT]] struct {
	Metadata Metadata `json:"metadata"`
	Data     T        `json:"data"`
	Type     Type     `json:"type"`
}

func (e Event[BT, T]) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0))
	if err != nil {
		return
	}
	err = e.Metadata.WriteBytes(w)
	if err != nil {
		return err
	}
	err = e.Data.WriteBytes(w)
	if err != nil {
		return err
	}
	err = e.Type.WriteBytes(w)
	if err != nil {
		return err
	}
	return nil
}

func (e *Event[BT, T]) ReadBytes(r io.Reader) (err error) {
	var v uint8
	err = bcts.ReadUInt8(r, &v)
	if err != nil {
		return
	}
	if v != 0 {
		return fmt.Errorf("invalid event version, %s=%d, %s=%d", "expected", 0, "got", v)
	}
	err = e.Metadata.ReadBytes(r)
	if err != nil {
		return
	}
	err = e.Data.ReadBytes(r)
	if err != nil {
		return
	}
	err = e.Type.ReadBytes(r)
	if err != nil {
		return
	}
	return nil
}

type ReadEvent[BT any, T bcts.ReadWriter[BT]] struct {
	Created time.Time `json:"created"`
	Event[BT, T]
	Position uint64 `json:"position"`
}

func (e ReadEvent[BT, T]) WriteBytes(w io.Writer) (err error) {
	err = bcts.WriteUInt8(w, uint8(0))
	if err != nil {
		return
	}
	err = e.Event.WriteBytes(w)
	if err != nil {
		return err
	}
	err = bcts.WriteTime(w, e.Created)
	if err != nil {
		return err
	}
	err = bcts.WriteUInt64(w, e.Position)
	if err != nil {
		return err
	}
	return nil
}

func (e *ReadEvent[BT, T]) ReadBytes(r io.Reader) (err error) {
	var v uint8
	err = bcts.ReadUInt8(r, &v)
	if err != nil {
		return
	}
	if v != 0 {
		return fmt.Errorf("invalid event version, %s=%d, %s=%d", "expected", 0, "got", v)
	}
	err = e.Event.ReadBytes(r)
	if err != nil {
		return
	}
	err = bcts.ReadTime(r, &e.Created)
	if err != nil {
		return err
	}
	err = bcts.ReadUInt64(r, &e.Position)
	if err != nil {
		return err
	}
	return nil
}

type ReadEventWAcc[BT any, T bcts.ReadWriter[BT]] struct {
	CTX context.Context
	Acc func()
	ReadEvent[BT, T]
}

type WriteEvent[BT any, T bcts.ReadWriter[BT]] struct {
	status chan store.WriteStatus
	event  Event[BT, T]
}

type WriteEventReadStatus[BT any, T bcts.ReadWriter[BT]] interface {
	Event() *Event[BT, T]
	Done() <-chan store.WriteStatus
	Close(store.WriteStatus)
	Store() *store.WriteEvent
	StatusChan() chan store.WriteStatus
}

func Map[OT, NT any, OOT bcts.ReadWriter[OT], NNT bcts.ReadWriter[NT]](
	e WriteEventReadStatus[OT, OOT],
	f func(OOT) NNT,
) WriteEventReadStatus[NT, NNT] {
	return &WriteEvent[NT, NNT]{
		event: Event[NT, NNT]{
			Type:     e.Event().Type,
			Data:     f(e.Event().Data),
			Metadata: e.Event().Metadata,
		},
		status: e.StatusChan(),
	}
}

func NewWriteEvent[BT any, T bcts.ReadWriter[BT]](
	e Event[BT, T],
) WriteEventReadStatus[BT, T] { // Dont think i like this
	return &WriteEvent[BT, T]{
		event:  e,
		status: make(chan store.WriteStatus, 1),
	}
}

func (e *WriteEvent[BT, T]) Event() *Event[BT, T] {
	return &e.event
}

func (e *WriteEvent[BT, T]) Done() <-chan store.WriteStatus {
	return e.status
}

func (e *WriteEvent[BT, T]) Close(status store.WriteStatus) {
	if e.status == nil {
		return
	}
	e.status <- status
	close(e.status)
}

func (e *WriteEvent[BT, T]) StatusChan() chan store.WriteStatus {
	return e.status
}

func (e *WriteEvent[BT, T]) Store() *store.WriteEvent {
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
	mBw := bufio.NewWriter(mByte)
	err := e.event.Metadata.WriteBytes(mBw)
	if err == nil {
		err = mBw.Flush()
	}
	if err != nil {
		log.WithError(err).Error("while marshaling metadata")
		e.Close(store.WriteStatus{
			Error: err,
		})
		return nil
	}

	dByte := bytes.NewBuffer([]byte{})
	dBw := bufio.NewWriter(dByte)
	err = e.event.Data.WriteBytes(dBw)
	if err == nil {
		err = dBw.Flush()
	}
	// dByte, err := json.Marshal(e.event.Data)
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
			Data:     dByte.Bytes(),
			Metadata: mByte.Bytes(),
		},
		Status: e.status,
	}
}

type (
	ByteEvent      Event[bcts.Bytes, *bcts.Bytes]
	ByteWriteEvent WriteEvent[bcts.Bytes, *bcts.Bytes]
	ByteReadEvent  ReadEvent[bcts.Bytes, *bcts.Bytes]
)
