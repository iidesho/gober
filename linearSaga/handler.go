package saga

import (
	"context"

	"github.com/iidesho/gober/bcts"
)

type Handler[BT any, T bcts.ReadWriter[BT]] interface {
	Execute(T, context.Context) error
	Reduce(T, context.Context) error
}

type handlerBuilder[BT any, T bcts.ReadWriter[BT]] struct {
	execute func(T, context.Context) error
	reduce  func(T, context.Context) error
}

type DumHandler[BT any, T bcts.ReadWriter[BT]] struct {
	ExecuteFunc func(T, context.Context) error
	ReduceFunc  func(T, context.Context) error
}

type emptyHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Execute(execute func(v T, ctx context.Context) error) executeHandlerBuilder[BT, T]
}
type executeHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Reduce(reduce func(v T, ctx context.Context) error) executeReduceHandlerBuilder[BT, T]
}
type executeReduceHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Build() Handler[BT, T]
}

func NewHandlerBuilder[BT any, T bcts.ReadWriter[BT]]() emptyHandlerBuilder[BT, T] {
	return handlerBuilder[BT, T]{}
}

func (h handlerBuilder[BT, T]) Execute(
	execute func(v T, ctx context.Context) error,
) executeHandlerBuilder[BT, T] {
	h.execute = execute
	return h
}

func (h handlerBuilder[BT, T]) Reduce(
	reduce func(v T, ctx context.Context) error,
) executeReduceHandlerBuilder[BT, T] {
	h.reduce = reduce
	return h
}

func (h handlerBuilder[BT, T]) Build() Handler[BT, T] {
	return DumHandler[BT, T]{
		ExecuteFunc: h.execute,
		ReduceFunc:  h.reduce,
	}
}

func (h DumHandler[BT, T]) Execute(v T, ctx context.Context) error {
	return h.ExecuteFunc(v, ctx)
}

func (h DumHandler[BT, T]) Reduce(v T, ctx context.Context) error {
	return h.ReduceFunc(v, ctx)
}

func NewDumHandler[BT any, T bcts.ReadWriter[BT]](
	execute func(T, context.Context) error,
	reduce func(T, context.Context) error,
) Handler[BT, T] {
	return DumHandler[BT, T]{
		ExecuteFunc: execute,
		ReduceFunc:  reduce,
	}
}
