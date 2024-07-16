package store

import (
	"math"
	"time"

	"github.com/gofrs/uuid"
)

type Event struct {
	Type     string    `json:"type"`
	Data     []byte    `json:"data"`
	Metadata []byte    `json:"metadata"`
	Id       uuid.UUID `json:"id"`
}

type ReadEvent struct {
	Created time.Time `json:"created"`
	Event
	Position uint64 `json:"position"`
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
