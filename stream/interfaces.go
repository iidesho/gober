package stream

import (
	"context"
	"github.com/cantara/gober/stream/event"

	"github.com/cantara/gober/store"
)

type Persistence interface {
	Store(streamName string, ctx context.Context, events ...store.Event) (transactionId uint64, err error)
	Stream(streamName string, from store.StreamPosition, ctx context.Context) (out <-chan store.Event, err error)
}

type Stream[DT, MT any] interface {
	Store(event event.Event[DT, MT], cryptoKey CryptoKeyProvider) (transactionId uint64, err error)
	Stream(eventTypes []event.Type, from store.StreamPosition, filter Filter[MT], cryptKey CryptoKeyProvider, ctx context.Context) (out <-chan event.Event[DT, MT], err error)
	Name() string
}
