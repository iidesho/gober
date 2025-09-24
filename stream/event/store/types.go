package store

import (
	"io"
	"math"
	"time"

	"github.com/gofrs/uuid"
	"github.com/iidesho/gober/bcts"
)

type Event struct {
	Type      string         `json:"type"`
	Data      []byte         `json:"data"`
	Metadata  []byte         `json:"metadata"`
	ID        uuid.UUID      `json:"id"`
	Shard     string         `json:"shard_key"`
	GlobalPos StreamPosition `json:"global_pos"`
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
	err = bcts.WriteStaticBytes(w, e.ID[:])
	if err != nil {
		return err
	}
	err = bcts.WriteTinyString(w, e.Shard)
	if err != nil {
		return err
	}
	err = bcts.WriteUInt64(w, e.GlobalPos)
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
	err = bcts.ReadStaticBytes(r, e.ID[:])
	if err != nil {
		return err
	}
	err = bcts.ReadTinyString(r, &e.Shard)
	if err != nil {
		return err
	}
	err = bcts.ReadUInt64(r, &e.GlobalPos)
	if err != nil {
		return err
	}
	return nil
}

type ReadEvent struct {
	Created time.Time `json:"created"`
	Event
	Position StreamPosition `json:"position"`
}

func (e ReadEvent) WriteBytes(w io.Writer) error {
	err := bcts.WriteTime(w, e.Created)
	if err != nil {
		return err
	}
	err = e.Event.WriteBytes(w)
	if err != nil {
		return err
	}
	err = bcts.WriteUInt64(w, e.Position)
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
	Position StreamPosition
}

type StreamPosition uint64

const (
	STREAM_START StreamPosition = 0
	STREAM_END   StreamPosition = math.MaxUint64
)
