package gober

import (
	"context"

	"github.com/cantara/gober/store"
)

type Persistence interface {
	Store(streamName string, ctx context.Context, events ...store.Event) (transactionId uint64, err error)
	Stream(streamName string, from store.StreamPosition, ctx context.Context) (out <-chan store.Event, err error)
}
