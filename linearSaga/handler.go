package saga

import "github.com/iidesho/gober/bcts"

type Handler[BT any, T bcts.ReadWriter[BT]] interface {
	Execute(T) (State, error)
	Reduce(T) (State, error)
}

type handlerBuilder[BT any, T bcts.ReadWriter[BT]] struct {
	execute func(T) (State, error)
	reduce  func(T) (State, error)
}

type DumHandler[BT any, T bcts.ReadWriter[BT]] struct {
	ExecuteFunc func(T) (State, error)
	ReduceFunc  func(T) (State, error)
}

type emptyHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Execute(execute func(v T) (State, error)) executeHandlerBuilder[BT, T]
}
type executeHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Reduce(reduce func(v T) (State, error)) executeReduceHandlerBuilder[BT, T]
}
type executeReduceHandlerBuilder[BT any, T bcts.ReadWriter[BT]] interface {
	Build() Handler[BT, T]
}

func NewHandlerBuilder[BT any, T bcts.ReadWriter[BT]]() emptyHandlerBuilder[BT, T] {
	return handlerBuilder[BT, T]{}
}

func (h handlerBuilder[BT, T]) Execute(
	execute func(v T) (State, error),
) executeHandlerBuilder[BT, T] {
	h.execute = execute
	return h
}

func (h handlerBuilder[BT, T]) Reduce(
	reduce func(v T) (State, error),
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

func (h DumHandler[BT, T]) Execute(v T) (State, error) {
	return h.ExecuteFunc(v)
}

func (h DumHandler[BT, T]) Reduce(v T) (State, error) {
	return h.ReduceFunc(v)
}

func NewDumHandler[BT any, T bcts.ReadWriter[BT]](
	execute func(T) (State, error),
	reduce func(T) (State, error),
) Handler[BT, T] {
	return DumHandler[BT, T]{
		ExecuteFunc: execute,
		ReduceFunc:  reduce,
	}
}
