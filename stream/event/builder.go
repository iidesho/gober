package event

import (
	"github.com/gofrs/uuid"
	log "github.com/iidesho/bragi/sbragi"
	"github.com/iidesho/gober/bcts"
)

type builder struct {
	Metadata Metadata
	Type     Type
	Data     []byte
	Id       uuid.UUID
}

type Builder interface {
	WithType(t Type) builder
	WithData(data []byte) builder
	WithMetadata(data Metadata) builder
	BuildRead() (ev ByteReadEvent, err error)
	BuildStore() (ev WriteEvent[bcts.Bytes, *bcts.Bytes], err error)
}

func NewBuilder() Builder {
	return builder{}
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

func (e builder) BuildRead() (ev ByteReadEvent, err error) {
	if e.Type == "" {
		log.Error("missing event type in builder")
		err = ErrInvalidType
		return
	}
	e.Metadata.EventType = e.Type
	d := bcts.Bytes(e.Data)
	ev = ByteReadEvent{
		Event: Event[bcts.Bytes, *bcts.Bytes]{
			Type:     e.Type,
			Data:     &d,
			Metadata: e.Metadata,
		},
	}
	return
}

func (e builder) BuildStore() (ev WriteEvent[bcts.Bytes, *bcts.Bytes], err error) {
	if e.Type == "" {
		log.Error("missing event type in builder")
		err = ErrInvalidType
		return
	}
	if e.Id.IsNil() {
		e.Id, err = uuid.NewV7()
		if err != nil {
			return
		}
	}
	e.Metadata.EventType = e.Type
	d := bcts.Bytes(e.Data)
	ev = WriteEvent[bcts.Bytes, *bcts.Bytes]{
		event: Event[bcts.Bytes, *bcts.Bytes]{
			Type:     e.Type,
			Data:     &d,
			Metadata: e.Metadata,
		},
	}
	return
}
