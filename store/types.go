package store

import (
	"github.com/cantara/gober/stream/event"
	"github.com/gofrs/uuid"
	"math"
	"time"
)

type Event struct {
	Id       uuid.UUID  `json:"id"`
	Type     event.Type `json:"type"`
	Data     []byte     `json:"data"`
	Metadata []byte     `json:"metadata"`

	Transaction uint64    `json:"transaction"`
	Position    uint64    `json:"position"`
	Created     time.Time `json:"created"`
}

type StreamPosition uint64

const (
	STREAM_START StreamPosition = 0
	STREAM_END   StreamPosition = math.MaxUint64
)
