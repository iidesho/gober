package state

type State[T any] interface {
	Execute(T) (State[T], error)
	Done(T) (bool, error)
	String() string
}
