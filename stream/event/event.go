package event

import (
	"fmt"
	"time"

	"github.com/gofrs/uuid"
)

type Event[DT, MT any] struct {
	Id       uuid.UUID    `json:"id"`
	Type     Type         `json:"type"`
	Data     DT           `json:"data"`
	Metadata Metadata[MT] `json:"metadata"`

	Transaction uint64    `json:"transaction"`
	Position    uint64    `json:"position"`
	Created     time.Time `json:"created"`
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

type Metadata[MT any] struct {
	Stream    string    `json:"stream"`
	EventType Type      `json:"event_type"`
	Version   string    `json:"version"`
	DataType  string    `json:"data_type"`
	Key       string    `json:"key"` //Strictly used for things like getting the cryptoKey
	Event     MT        `json:"event"`
	Created   time.Time `json:"created"`
}

type builder[DT, MT any] struct {
	Id       uuid.UUID
	Type     Type
	Data     DT
	Metadata Metadata[MT]
}

type Builder[DT, MT any] interface {
	WithId(id uuid.UUID) builder[DT, MT]
	WithType(t Type) builder[DT, MT]
	WithData(data DT) builder[DT, MT]
	WithMetadata(data Metadata[MT]) builder[DT, MT]
	Build() (ev Event[DT, MT], err error)
}

func NewBuilder[DT, MT any]() Builder[DT, MT] {
	return builder[DT, MT]{}
}

func (e builder[DT, MT]) WithId(id uuid.UUID) builder[DT, MT] { //This was a function that validated strings as uuids.
	e.Id = id
	return e
}

func (e builder[DT, MT]) WithType(t Type) builder[DT, MT] {
	e.Type = t
	return e
}

func (e builder[DT, MT]) WithData(data DT) builder[DT, MT] {
	e.Data = data
	return e
}

func (e builder[DT, MT]) WithMetadata(data Metadata[MT]) builder[DT, MT] {
	e.Metadata = data
	return e
}

func (e builder[DT, MT]) Build() (ev Event[DT, MT], err error) {
	if e.Type == "" {
		err = MissingTypeError
		return
	}
	e.Metadata.EventType = e.Type
	ev = Event[DT, MT]{
		Id:       e.Id,
		Type:     e.Type,
		Data:     e.Data,
		Metadata: e.Metadata,
	}
	return
}

var MissingTypeError = fmt.Errorf("event type is missing")
