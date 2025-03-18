package saga

import "github.com/iidesho/gober/bcts"

type Handler[BT any, T bcts.ReadWriter[BT]] interface {
	Execute(T) error
	Reduce(T) error
}

type handlerBuilder[BT any, T bcts.ReadWriter[BT]] struct {
	execute func(T) error
	reduce  func(T) error
}

type DumHandler[BT any, T bcts.ReadWriter[BT]] struct {
	ExecuteFunc func(T) error
	ReduceFunc  func(T) error
}

type emptyHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Execute(execute func(v T) error) executeHandlerBuilder[BT, T]
}
type executeHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Reduce(reduce func(v T) error) executeReduceHandlerBuilder[BT, T]
}
type executeReduceHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Build() Handler[BT, T]
}

func NewHandlerBuilder[BT any, T bcts.ReadWriter[BT]]() emptyHandlerBuilder[BT, T] {
	return handlerBuilder[BT, T]{}
}

func (h handlerBuilder[BT, T]) Execute(
	execute func(v T) error,
) executeHandlerBuilder[BT, T] {
	h.execute = execute
	return h
}

func (h handlerBuilder[BT, T]) Reduce(
	reduce func(v T) error,
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

func (h DumHandler[BT, T]) Execute(v T) error {
	return h.ExecuteFunc(v)
}

func (h DumHandler[BT, T]) Reduce(v T) error {
	return h.ReduceFunc(v)
}

func NewDumHandler[BT any, T bcts.ReadWriter[BT]](
	execute func(T) error,
	reduce func(T) error,
) Handler[BT, T] {
	return DumHandler[BT, T]{
		ExecuteFunc: execute,
		ReduceFunc:  reduce,
	}
}
