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
}

type ReadEvent struct {
	Event

	Position uint64    `json:"position"`
	Created  time.Time `json:"created"`
}

type WriteEvent struct {
	Event

	Status chan<- event.WriteStatus
}

type StreamPosition uint64

const (
	STREAM_START StreamPosition = 0
	STREAM_END   StreamPosition = math.MaxUint64
)
