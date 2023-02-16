package event

import (
	"context"
	"time"
)

type Metadata struct {
	Stream    string         `json:"stream"`
	EventType Type           `json:"event_type"`
	Version   string         `json:"version"`
	DataType  string         `json:"data_type"`
	Key       string         `json:"key"` //Strictly used for things like getting the cryptoKey
	Extra     map[string]any `json:"extra"`
	Created   time.Time      `json:"created"`
}

type Event[T any] struct {
	Type     Type     `json:"type"`
	Data     T        `json:"data"`
	Metadata Metadata `json:"metadata"`
}

type ReadEvent[T any] struct {
	Event[T]

	Position uint64    `json:"position"`
	Created  time.Time `json:"created"`
}

type ReadEventWAcc[T any] struct {
	ReadEvent[T]

	Acc func()
	CTX context.Context
}

type WriteEvent[T any] struct {
	Event[T]

	Status chan<- WriteStatus
}

type WriteStatus struct {
	Error    error
	Position uint64
	Time     time.Time
}

type WriteEventReadStatus[T any] interface {
	Event() Event[T]
	Done() <-chan struct{}
	Close()
}

func NewWriteEvent[T any](e Event[T]) WriteEventReadStatus[T] {
	return &writeEvent[T]{
		event: e,
		done:  make(chan struct{}, 1),
	}
}

type writeEvent[T any] struct {
	event Event[T]
	done  chan struct{}
}

func (e *writeEvent[T]) Event() Event[T] {
	return e.event
}

func (e *writeEvent[T]) Done() <-chan struct{} {
	return e.done
}

func (e *writeEvent[T]) Close() {
	close(e.done)
}

type ByteEvent Event[[]byte]
type ByteWriteEvent WriteEvent[[]byte]
type ByteReadEvent ReadEvent[[]byte]
