package store

import (
	"io"
	"math"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/gober/bcts"
)

type Event struct {
	Type     string    `json:"type"`
	Data     []byte    `json:"data"`
	Metadata []byte    `json:"metadata"`
	Id       uuid.UUID `json:"id"`
}

func (e Event) WriteBytes(w io.Writer) error {
	err := bcts.WriteSmallString(w, e.Type)
	if err != nil {
		return err
	}
	err = bcts.WriteBytes(w, e.Data)
	if err != nil {
		return err
	}
	err = bcts.WriteBytes(w, e.Metadata)
	if err != nil {
		return err
	}
	err = bcts.WriteStaticBytes(w, e.Id[:])
	if err != nil {
		return err
	}
	return nil
}

func (e *Event) ReadBytes(r io.Reader) error {
	err := bcts.ReadSmallString(r, &e.Type)
	if err != nil {
		return err
	}
	err = bcts.ReadBytes(r, &e.Data)
	if err != nil {
		return err
	}
	err = bcts.ReadBytes(r, &e.Metadata)
	if err != nil {
		return err
	}
	err = bcts.ReadStaticBytes(r, e.Id[:])
	if err != nil {
		return err
	}
	return nil
}

type ReadEvent struct {
	Created time.Time `json:"created"`
	Event
	Position uint64 `json:"position"`
}

func (r ReadEvent) WriteBytes(w io.Writer) error {
	err := bcts.WriteTime(w, r.Created)
	if err != nil {
		return err
	}
	err = r.Event.WriteBytes(w)
	if err != nil {
		return err
	}
	err = bcts.WriteUInt64(w, r.Position)
	if err != nil {
		return err
	}
	return nil
}

func (e *ReadEvent) ReadBytes(r io.Reader) error {
	err := bcts.ReadTime(r, &e.Created)
	if err != nil {
		return err
	}

	e.Event, err = bcts.ReadReader[Event](r)
	// err = e.Event.ReadBytes(r)
	if err != nil {
		return err
	}
	err = bcts.ReadUInt64(r, &e.Position)
	if err != nil {
		return err
	}
	return nil
}

type WriteEvent struct {
	Status chan<- WriteStatus
	Event
}

type WriteStatus struct {
	Time     time.Time
	Error    error
	Position uint64
}

type StreamPosition uint64

const (
	STREAM_START StreamPosition = 0
	STREAM_END   StreamPosition = math.MaxUint64
)
