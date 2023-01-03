package event

import (
	"fmt"
	"time"

	"github.com/gofrs/uuid"
)

type StoreEvent struct {
	Id       uuid.UUID `json:"id"`
	Type     Type      `json:"type"`
	Data     any       `json:"data"`
	Metadata Metadata  `json:"metadata"`
}

type Event[DT any] struct {
	Id       uuid.UUID `json:"id"`
	Type     Type      `json:"type"`
	Data     DT        `json:"data"`
	Metadata Metadata  `json:"metadata"`

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

type builder[DT any] struct {
	Id       uuid.UUID
	Type     Type
	Data     DT
	Metadata Metadata
}

type Builder[DT any] interface {
	WithId(id uuid.UUID) builder[DT]
	WithType(t Type) builder[DT]
	WithData(data DT) builder[DT]
	WithMetadata(data Metadata) builder[DT]
	Build() (ev Event[DT], err error)
	BuildStore() (ev StoreEvent, err error)
}

func NewBuilder[DT any]() Builder[DT] {
	return builder[DT]{}
}

func (e builder[DT]) WithId(id uuid.UUID) builder[DT] { //This was a function that validated strings as uuids.
	e.Id = id
	return e
}

func (e builder[DT]) WithType(t Type) builder[DT] {
	e.Type = t
	return e
}

func (e builder[DT]) WithData(data DT) builder[DT] {
	e.Data = data
	return e
}

func (e builder[DT]) WithMetadata(data Metadata) builder[DT] {
	e.Metadata = data
	return e
}

func (e builder[DT]) Build() (ev Event[DT], err error) {
	if e.Type == "" {
		err = InvalidTypeError
		return
	}
	e.Metadata.EventType = e.Type
	ev = Event[DT]{
		Id:       e.Id,
		Type:     e.Type,
		Data:     e.Data,
		Metadata: e.Metadata,
	}
	return
}

func (e builder[DT]) BuildStore() (ev StoreEvent, err error) {
	if e.Type == "" {
		err = InvalidTypeError
		return
	}
	e.Metadata.EventType = e.Type
	ev = StoreEvent{
		Id:       e.Id,
		Type:     e.Type,
		Data:     e.Data,
		Metadata: e.Metadata,
	}
	return
}

var InvalidTypeError = fmt.Errorf("event type is invalid")
