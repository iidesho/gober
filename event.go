package gober

import (
	"time"

	"github.com/gofrs/uuid"
)

type Event[DT, MT any] struct {
	Id       uuid.UUID    `json:"id"`
	Type     string       `json:"type"`
	Data     DT           `json:"data"`
	Metadata Metadata[MT] `json:"metadata"`

	Transaction uint64    `json:"transaction"`
	Position    uint64    `json:"position"`
	Created     time.Time `json:"created"`
}

type Metadata[MT any] struct {
	Stream   string    `json:"stream"`
	Type     string    `json:"type"`
	Version  string    `json:"version"`
	DataType string    `json:"data_type"`
	Key      string    `json:"key"` //Strictly used for things like getting the cryptoKey
	Event    MT        `json:"event"`
	Created  time.Time `json:"created"`
}

type eventBuilder[DT, MT any] struct {
	Id       uuid.UUID
	Type     string
	Data     DT
	Metadata Metadata[MT]
}

func EventBuilder[DT, MT any]() eventBuilder[DT, MT] {
	return eventBuilder[DT, MT]{}
}

func (e eventBuilder[DT, MT]) WithId(id uuid.UUID) eventBuilder[DT, MT] { //This was a function that validated strings as uuids.
	e.Id = id
	return e
}

func (e eventBuilder[DT, MT]) WithType(t string) eventBuilder[DT, MT] {
	e.Type = t
	return e
}

func (e eventBuilder[DT, MT]) WithData(data DT) eventBuilder[DT, MT] {
	e.Data = data
	return e
}

func (e eventBuilder[DT, MT]) WithMetadata(data Metadata[MT]) eventBuilder[DT, MT] {
	e.Metadata = data
	return e
}

func (e eventBuilder[DT, MT]) Build() (ev Event[DT, MT], err error) {
	if e.Type == "" {
		err = MissingTypeError
		return
	}
	ev = Event[DT, MT]{
		Id:       e.Id,
		Type:     e.Type,
		Data:     e.Data,
		Metadata: e.Metadata,
	}
	return
}
