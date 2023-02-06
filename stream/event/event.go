package event

import (
	"fmt"
	"time"

	"github.com/gofrs/uuid"
)

type Event struct {
	Id       uuid.UUID `json:"id"`
	Type     Type      `json:"type"`
	Data     []byte    `json:"data"`
	Metadata Metadata  `json:"metadata"`
}

type ReadEvent struct {
	Event

	//Transaction uint64    `json:"transaction"`
	Position uint64    `json:"position"`
	Created  time.Time `json:"created"`
}

type Type string

const (
	Create  Type = "create"
	Update  Type = "update"
	Delete  Type = "delete"
	Invalid Type = "invalid"
)

func AllTypes() []Type {
	return []Type{Create, Update, Delete}
}

func TypeFromString(s string) Type {
	switch s {
	case string(Create):
		return Create
	case string(Update):
		return Update
	case string(Delete):
		return Delete
	}
	return Invalid
}

type Metadata struct {
	Stream    string         `json:"stream"`
	EventType Type           `json:"event_type"`
	Version   string         `json:"version"`
	DataType  string         `json:"data_type"`
	Key       string         `json:"key"` //Strictly used for things like getting the cryptoKey
	Extra     map[string]any `json:"extra"`
	Created   time.Time      `json:"created"`
}

type builder struct {
	Id       uuid.UUID
	Type     Type
	Data     []byte
	Metadata Metadata
}

type Builder interface {
	WithId(id uuid.UUID) builder
	WithType(t Type) builder
	WithData(data []byte) builder
	WithMetadata(data Metadata) builder
	BuildRead() (ev ReadEvent, err error)
	BuildStore() (ev Event, err error)
}

func NewBuilder() Builder {
	return builder{}
}

func (e builder) WithId(id uuid.UUID) builder { //This was a function that validated strings as uuids.
	e.Id = id
	return e
}

func (e builder) WithType(t Type) builder {
	e.Type = t
	return e
}

func (e builder) WithData(data []byte) builder {
	e.Data = data
	return e
}

func (e builder) WithMetadata(data Metadata) builder {
	e.Metadata = data
	return e
}

func (e builder) BuildRead() (ev ReadEvent, err error) {
	if e.Type == "" {
		err = InvalidTypeError
		return
	}
	e.Metadata.EventType = e.Type
	ev = ReadEvent{
		Event: Event{
			Id:       e.Id,
			Type:     e.Type,
			Data:     e.Data,
			Metadata: e.Metadata,
		},
	}
	return
}

func (e builder) BuildStore() (ev Event, err error) {
	if e.Type == "" {
		err = InvalidTypeError
		return
	}
	if e.Id.IsNil() {
		e.Id, err = uuid.NewV7()
		if err != nil {
			return
		}
	}
	e.Metadata.EventType = e.Type
	ev = Event{
		Id:       e.Id,
		Type:     e.Type,
		Data:     e.Data,
		Metadata: e.Metadata,
	}
	return
}

var InvalidTypeError = fmt.Errorf("event type is invalid")
